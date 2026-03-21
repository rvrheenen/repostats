"""Background data collector: git log parsing, cloc, file coupling."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
from datetime import datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path

from repostats.config import AppConfig, RepoConfig
from repostats.db import Database
from repostats.mailmap import EmailNormalizer, Mailmap
from repostats.models import CommitRecord, FileChange

logger = logging.getLogger(__name__)

MAX_CONCURRENT_SCANS = 4
COUPLING_MAX_FILES = 30


def check_cloc_available() -> bool:
    """Check if cloc is available in PATH."""
    return shutil.which("cloc") is not None


# ------------------------------------------------------------------
# Git subprocess helpers
# ------------------------------------------------------------------


async def _run_git(repo_path: str, *args: str) -> tuple[int, str]:
    """Run a git command and return (returncode, stdout)."""
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", repo_path, *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    if proc.returncode != 0 and stderr_bytes:
        logger.debug("git stderr: %s", stderr_bytes.decode(errors="replace").strip())
    return proc.returncode or 0, stdout_bytes.decode(errors="replace")


async def _get_total_commit_count(repo_path: str) -> int:
    """Get total non-merge commit count for progress tracking."""
    rc, out = await _run_git(repo_path, "rev-list", "--no-merges", "--count", "HEAD")
    if rc != 0:
        return 0
    stripped = out.strip()
    return int(stripped) if stripped.isdigit() else 0


# ------------------------------------------------------------------
# Git log parsing
# ------------------------------------------------------------------


def parse_git_log(
    output: str, repo_name: str
) -> tuple[list[CommitRecord], list[FileChange]]:
    """Parse git log output with null-byte-delimited format + numstat.

    Expected git command:
        git log --no-merges --format='%H%x00%an%x00%ae%x00%aI' --numstat
    """
    commits: list[CommitRecord] = []
    file_changes: list[FileChange] = []

    current_hash = ""
    current_author = ""
    current_email = ""
    current_date = ""
    current_insertions = 0
    current_deletions = 0
    current_files = 0

    def _save_current() -> None:
        nonlocal current_hash
        if current_hash:
            commits.append(CommitRecord(
                hash=current_hash, repo=repo_name,
                author=current_author, email=current_email,
                date=current_date, insertions=current_insertions,
                deletions=current_deletions, files_changed=current_files,
            ))
            current_hash = ""

    for line in output.splitlines():
        line = line.rstrip()
        if not line:
            continue

        # Commit header: hash\0author\0email\0date
        if "\x00" in line:
            _save_current()
            parts = line.split("\x00")
            if len(parts) >= 4:
                current_hash = parts[0]
                current_author = parts[1]
                current_email = parts[2]
                current_date = parts[3]
                current_insertions = 0
                current_deletions = 0
                current_files = 0
            continue

        # numstat line: insertions\tdeletions\tfile_path
        tab_parts = line.split("\t")
        if len(tab_parts) >= 3:
            ins_str, del_str = tab_parts[0], tab_parts[1]
            file_path = "\t".join(tab_parts[2:])  # file paths could contain tabs

            # Binary files show "-" for insertions/deletions
            try:
                ins = int(ins_str) if ins_str != "-" else 0
                dels = int(del_str) if del_str != "-" else 0
            except ValueError:
                continue

            # Handle rename syntax ({old => new} or old => new)
            if "{" in file_path and "}" in file_path and " => " in file_path:
                brace_start = file_path.index("{")
                brace_end = file_path.index("}")
                inner = file_path[brace_start + 1 : brace_end]
                _, new_part = inner.split(" => ", 1)
                file_path = file_path[:brace_start] + new_part + file_path[brace_end + 1 :]
            elif " => " in file_path:
                file_path = file_path.split(" => ", 1)[1]

            current_insertions += ins
            current_deletions += dels
            current_files += 1

            file_changes.append(FileChange(
                repo=repo_name, commit_hash=current_hash,
                file_path=file_path, insertions=ins, deletions=dels,
            ))

    _save_current()
    return commits, file_changes


# ------------------------------------------------------------------
# Author normalization
# ------------------------------------------------------------------


def _normalize_commits(
    commits: list[CommitRecord], mailmap: Mailmap | None
) -> None:
    """Apply author normalization in-place."""
    if mailmap:
        for c in commits:
            name, email = mailmap.resolve(c.author, c.email)
            c.author = name
            c.email = email
    else:
        normalizer = EmailNormalizer()
        for c in commits:
            normalizer.observe(c.author, c.email, c.date)
        for c in commits:
            name, email = normalizer.resolve(c.author, c.email)
            c.author = name
            c.email = email


# ------------------------------------------------------------------
# Database write helpers
# ------------------------------------------------------------------


async def _insert_commits(
    db: Database,
    commits: list[CommitRecord],
    file_changes: list[FileChange],
) -> None:
    """Bulk insert commits and file changes in a single transaction."""
    async with db.write_lock:
        try:
            await db.writer.executemany(
                """INSERT OR IGNORE INTO commits
                   (hash, repo, author, email, date, insertions, deletions, files_changed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [(c.hash, c.repo, c.author, c.email, c.date,
                  c.insertions, c.deletions, c.files_changed) for c in commits],
            )
            await db.writer.executemany(
                """INSERT OR IGNORE INTO file_changes
                   (repo, commit_hash, file_path, insertions, deletions)
                   VALUES (?, ?, ?, ?, ?)""",
                [(fc.repo, fc.commit_hash, fc.file_path,
                  fc.insertions, fc.deletions) for fc in file_changes],
            )
            await db.writer.commit()
        except Exception:
            await db.writer.rollback()
            raise


async def _update_scan_log(
    db: Database,
    repo: str,
    *,
    last_hash: str | None = None,
    commit_count: int = 0,
    total_commits: int | None = None,
    status: str = "done",
) -> None:
    """Upsert scan_log entry."""
    now = datetime.now(timezone.utc).isoformat()
    async with db.write_lock:
        await db.writer.execute(
            """INSERT INTO scan_log
                   (repo, last_commit_hash, last_scanned_at, commit_count, total_commits, status)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(repo) DO UPDATE SET
                   last_commit_hash = COALESCE(?, last_commit_hash),
                   last_scanned_at = ?,
                   commit_count = ?,
                   total_commits = COALESCE(?, total_commits),
                   status = ?""",
            (repo, last_hash, now, commit_count, total_commits, status,
             last_hash, now, commit_count, total_commits, status),
        )
        await db.writer.commit()


async def _get_last_hash(db: Database, repo: str) -> str | None:
    """Get last scanned commit hash for a repo."""
    async with db.reader.execute(
        "SELECT last_commit_hash FROM scan_log WHERE repo = ?", (repo,),
    ) as cur:
        row = await cur.fetchone()
    return row["last_commit_hash"] if row else None


async def _clear_repo_data(db: Database, repo: str) -> None:
    """Clear all data for a repo (for force-push recovery)."""
    async with db.write_lock:
        await db.writer.execute("DELETE FROM file_coupling WHERE repo = ?", (repo,))
        await db.writer.execute("DELETE FROM file_changes WHERE repo = ?", (repo,))
        await db.writer.execute("DELETE FROM commits WHERE repo = ?", (repo,))
        await db.writer.execute("DELETE FROM cloc_snapshots WHERE repo = ?", (repo,))
        await db.writer.execute("DELETE FROM scan_log WHERE repo = ?", (repo,))
        await db.writer.commit()
    logger.info("cleared all data for repo %s (force-push recovery)", repo)


# ------------------------------------------------------------------
# File coupling
# ------------------------------------------------------------------


async def compute_file_coupling(db: Database, repo: str) -> None:
    """Compute file coupling pairs from file_changes.

    Excludes commits touching more than COUPLING_MAX_FILES files.
    Replaces existing coupling data for the repo.
    """
    sql = """
        SELECT fc.commit_hash, fc.file_path
        FROM file_changes fc
        WHERE fc.repo = ? AND fc.commit_hash IN (
            SELECT commit_hash FROM file_changes
            WHERE repo = ?
            GROUP BY commit_hash
            HAVING COUNT(*) BETWEEN 2 AND ?
        )
        ORDER BY fc.commit_hash
    """
    async with db.reader.execute(sql, (repo, repo, COUPLING_MAX_FILES)) as cur:
        rows = await cur.fetchall()

    # Group file paths by commit
    commit_files: dict[str, list[str]] = {}
    for row in rows:
        commit_hash: str = row["commit_hash"]
        file_path: str = row["file_path"]
        commit_files.setdefault(commit_hash, []).append(file_path)

    # Count co-occurrences
    pair_counts: dict[tuple[str, str], int] = {}
    for files in commit_files.values():
        sorted_files = sorted(files)
        for a, b in combinations(sorted_files, 2):
            key = (a, b)
            pair_counts[key] = pair_counts.get(key, 0) + 1

    # Replace coupling data
    async with db.write_lock:
        await db.writer.execute("DELETE FROM file_coupling WHERE repo = ?", (repo,))
        if pair_counts:
            await db.writer.executemany(
                """INSERT INTO file_coupling (repo, file_a, file_b, coupling_count)
                   VALUES (?, ?, ?, ?)""",
                [(repo, a, b, count) for (a, b), count in pair_counts.items()],
            )
        await db.writer.commit()

    logger.info("computed %d coupling pairs for %s", len(pair_counts), repo)


# ------------------------------------------------------------------
# cloc
# ------------------------------------------------------------------


async def run_cloc(db: Database, repo_name: str, repo_path: str) -> None:
    """Run cloc and insert a language snapshot."""
    proc = await asyncio.create_subprocess_exec(
        "cloc", "--json", "--quiet", repo_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, _ = await proc.communicate()
    if proc.returncode != 0:
        logger.warning("cloc failed for %s", repo_name)
        return

    try:
        data: dict[str, object] = json.loads(stdout_bytes.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("failed to parse cloc output for %s", repo_name)
        return

    now = datetime.now(timezone.utc).isoformat()
    rows: list[tuple[str, str, int, int, int, int, str]] = []
    for lang, stats in data.items():
        if lang in ("header", "SUM"):
            continue
        if not isinstance(stats, dict):
            continue
        rows.append((
            repo_name, lang,
            stats.get("code", 0),  # type: ignore[arg-type]
            stats.get("comment", 0),  # type: ignore[arg-type]
            stats.get("blank", 0),  # type: ignore[arg-type]
            stats.get("nFiles", 0),  # type: ignore[arg-type]
            now,
        ))

    if rows:
        async with db.write_lock:
            await db.writer.executemany(
                """INSERT INTO cloc_snapshots
                       (repo, language, code, comment, blank, files, scanned_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            await db.writer.commit()
        logger.info("cloc snapshot: %d languages for %s", len(rows), repo_name)


# ------------------------------------------------------------------
# Repo scanning
# ------------------------------------------------------------------


async def scan_repo(
    db: Database,
    repo: RepoConfig,
    mailmap: Mailmap | None,
    history_depth_days: int,
    cloc_available: bool,
) -> None:
    """Scan a single repo: full or incremental."""
    repo_name = repo.name
    repo_path = repo.path

    if not Path(repo_path).is_dir() or not (Path(repo_path) / ".git").exists():
        logger.warning("skipping %s: not a valid git repo at %s", repo_name, repo_path)
        return

    logger.info("scanning %s at %s", repo_name, repo_path)

    total = await _get_total_commit_count(repo_path)

    # Preserve existing commit_count so the UI doesn't flicker during scanning
    async with db.reader.execute(
        "SELECT commit_count FROM scan_log WHERE repo = ?", (repo_name,),
    ) as cur:
        existing = await cur.fetchone()
    prior_count: int = existing["commit_count"] if existing else 0

    await _update_scan_log(
        db, repo_name, total_commits=total, status="scanning", commit_count=prior_count,
    )

    try:
        last_hash = await _get_last_hash(db, repo_name)
        git_args = [
            "log", "--no-merges",
            "--format=%H%x00%an%x00%ae%x00%aI",
            "--numstat",
        ]

        if last_hash:
            # Verify the hash is still reachable from HEAD (detects force-push/rebase)
            rc, _ = await _run_git(
                repo_path, "merge-base", "--is-ancestor", last_hash, "HEAD",
            )
            if rc != 0:
                logger.warning(
                    "last hash %s unreachable in %s, doing full re-scan",
                    last_hash[:12], repo_name,
                )
                await _clear_repo_data(db, repo_name)
                last_hash = None
                await _update_scan_log(
                    db, repo_name, total_commits=total, status="scanning", commit_count=0,
                )
            else:
                git_args.append(f"{last_hash}..HEAD")

        if not last_hash and history_depth_days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=history_depth_days)
            git_args.append(f"--after={cutoff.isoformat()}")

        rc, output = await _run_git(repo_path, *git_args)
        if rc != 0:
            logger.error("git log failed for %s (rc=%d)", repo_name, rc)
            await _update_scan_log(db, repo_name, status="error")
            return

        commits, file_changes = parse_git_log(output, repo_name)

        if not commits:
            logger.info("no new commits for %s", repo_name)
            await _update_scan_log(
                db, repo_name, commit_count=total, total_commits=total, status="done",
            )
            return

        _normalize_commits(commits, mailmap)
        await _insert_commits(db, commits, file_changes)

        # git log outputs newest first — first commit is HEAD
        new_head = commits[0].hash

        # Get total commit count from DB
        async with db.reader.execute(
            "SELECT COUNT(*) AS cnt FROM commits WHERE repo = ?", (repo_name,),
        ) as cur:
            row = await cur.fetchone()
        db_count: int = row["cnt"] if row else 0

        await _update_scan_log(
            db, repo_name,
            last_hash=new_head,
            commit_count=db_count,
            total_commits=total,
            status="done",
        )

        await compute_file_coupling(db, repo_name)

        if cloc_available:
            await run_cloc(db, repo_name, repo_path)

        logger.info(
            "scan complete for %s: %d new commits (%d total in DB)",
            repo_name, len(commits), db_count,
        )

    except Exception:
        logger.exception("error scanning %s", repo_name)
        await _update_scan_log(db, repo_name, status="error")


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------


async def scan_all(db: Database, config: AppConfig, cloc_available: bool) -> None:
    """Scan all configured repos concurrently."""
    if not config.repos:
        logger.warning("no repos configured")
        return

    mailmap: Mailmap | None = None
    if config.settings.mailmap_path:
        mailmap = Mailmap.from_file(config.settings.mailmap_path)

    sem = asyncio.Semaphore(MAX_CONCURRENT_SCANS)

    async def _bounded_scan(repo: RepoConfig) -> None:
        async with sem:
            await scan_repo(
                db, repo, mailmap, config.settings.history_depth_days, cloc_available,
            )

    await asyncio.gather(*[_bounded_scan(r) for r in config.repos])
    logger.info("scan cycle complete for %d repos", len(config.repos))
