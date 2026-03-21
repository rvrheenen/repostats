"""Tests for git log parsing and author normalization."""

from __future__ import annotations

from repostats.collector import parse_git_log
from repostats.mailmap import EmailNormalizer, Mailmap
from repostats.models import CommitRecord


# ------------------------------------------------------------------
# parse_git_log
# ------------------------------------------------------------------


def test_parse_single_commit() -> None:
    """Parse a single commit with numstat lines."""
    output = (
        "abc123\x00Alice\x00alice@example.com\x002024-01-15T10:30:00+00:00\n"
        "10\t5\tsrc/main.py\n"
        "3\t1\tREADME.md\n"
    )
    commits, file_changes = parse_git_log(output, "myrepo")

    assert len(commits) == 1
    c = commits[0]
    assert c.hash == "abc123"
    assert c.repo == "myrepo"
    assert c.author == "Alice"
    assert c.email == "alice@example.com"
    assert c.date == "2024-01-15T10:30:00+00:00"
    assert c.insertions == 13
    assert c.deletions == 6
    assert c.files_changed == 2

    assert len(file_changes) == 2
    assert file_changes[0].file_path == "src/main.py"
    assert file_changes[0].insertions == 10
    assert file_changes[0].deletions == 5
    assert file_changes[1].file_path == "README.md"


def test_parse_multiple_commits() -> None:
    """Parse multiple commits separated by blank lines."""
    output = (
        "aaa111\x00Alice\x00alice@ex.com\x002024-01-10T09:00:00+00:00\n"
        "5\t2\tfile1.py\n"
        "\n"
        "bbb222\x00Bob\x00bob@ex.com\x002024-01-11T10:00:00+00:00\n"
        "1\t0\tfile2.py\n"
    )
    commits, file_changes = parse_git_log(output, "repo")

    assert len(commits) == 2
    assert commits[0].hash == "aaa111"
    assert commits[0].author == "Alice"
    assert commits[1].hash == "bbb222"
    assert commits[1].author == "Bob"
    assert len(file_changes) == 2


def test_parse_empty_output() -> None:
    """Empty git log output produces no commits."""
    commits, file_changes = parse_git_log("", "repo")
    assert commits == []
    assert file_changes == []


def test_parse_binary_files() -> None:
    """Binary files show '-' for insertions/deletions."""
    output = (
        "abc123\x00Alice\x00a@b.com\x002024-01-15T10:30:00+00:00\n"
        "-\t-\timage.png\n"
        "5\t2\tcode.py\n"
    )
    commits, file_changes = parse_git_log(output, "repo")

    assert len(commits) == 1
    assert commits[0].insertions == 5  # binary file counted as 0
    assert commits[0].deletions == 2
    assert commits[0].files_changed == 2

    assert file_changes[0].insertions == 0
    assert file_changes[0].deletions == 0
    assert file_changes[1].insertions == 5


def test_parse_rename_brace_syntax() -> None:
    """Rename with brace syntax: src/{old => new}/file.py."""
    output = (
        "abc123\x00Alice\x00a@b.com\x002024-01-15T10:30:00+00:00\n"
        "2\t1\tsrc/{old => new}/file.py\n"
    )
    _, file_changes = parse_git_log(output, "repo")

    assert len(file_changes) == 1
    assert file_changes[0].file_path == "src/new/file.py"


def test_parse_rename_arrow_syntax() -> None:
    """Rename with arrow syntax: old.py => new.py."""
    output = (
        "abc123\x00Alice\x00a@b.com\x002024-01-15T10:30:00+00:00\n"
        "0\t0\told.py => new.py\n"
    )
    _, file_changes = parse_git_log(output, "repo")

    assert len(file_changes) == 1
    assert file_changes[0].file_path == "new.py"


def test_parse_commit_no_files() -> None:
    """Commit with header but no numstat lines (e.g., empty commit)."""
    output = "abc123\x00Alice\x00a@b.com\x002024-01-15T10:30:00+00:00\n"
    commits, file_changes = parse_git_log(output, "repo")

    assert len(commits) == 1
    assert commits[0].insertions == 0
    assert commits[0].files_changed == 0
    assert file_changes == []


# ------------------------------------------------------------------
# Author normalization
# ------------------------------------------------------------------


def test_normalize_with_email_normalizer() -> None:
    """EmailNormalizer groups by email and uses most recent name."""
    normalizer = EmailNormalizer()
    normalizer.observe("Alice", "alice@ex.com", "2024-01-01T00:00:00")
    normalizer.observe("alice smith", "alice@ex.com", "2024-06-01T00:00:00")
    normalizer.observe("Bob", "bob@ex.com", "2024-03-01T00:00:00")

    name, email = normalizer.resolve("Alice", "alice@ex.com")
    assert name == "alice smith"
    assert email == "alice@ex.com"

    name, email = normalizer.resolve("Bob", "bob@ex.com")
    assert name == "Bob"


def test_normalize_email_case_insensitive() -> None:
    """Email matching is case-insensitive."""
    normalizer = EmailNormalizer()
    normalizer.observe("Alice", "Alice@Ex.com", "2024-01-01T00:00:00")

    name, email = normalizer.resolve("alice", "ALICE@EX.COM")
    assert name == "Alice"
    assert email == "alice@ex.com"


def test_normalize_unknown_email() -> None:
    """Unknown email returns original name and lowercase email."""
    normalizer = EmailNormalizer()
    name, email = normalizer.resolve("Charlie", "Charlie@Test.COM")
    assert name == "Charlie"
    assert email == "charlie@test.com"
