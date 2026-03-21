"""Author identity normalization via .mailmap and email-based fallback."""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"<([^>]+)>")


class Mailmap:
    """Parses a .mailmap file and normalizes author name/email pairs.

    Supported formats:
        Proper Name <proper@email>
        <proper@email> <commit@email>
        Proper Name <proper@email> <commit@email>
        Proper Name <proper@email> Commit Name <commit@email>
    """

    def __init__(self) -> None:
        self._by_name_email: dict[tuple[str, str], tuple[str, str]] = {}
        self._by_email: dict[str, tuple[str, str]] = {}

    @classmethod
    def from_file(cls, path: str | Path) -> Mailmap:
        """Load mappings from a .mailmap file."""
        mm = cls()
        p = Path(path)
        if not p.is_file():
            logger.warning("mailmap file not found: %s", path)
            return mm

        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            mm._parse_line(line)

        logger.info(
            "loaded %d mailmap entries from %s",
            len(mm._by_email) + len(mm._by_name_email),
            path,
        )
        return mm

    def _parse_line(self, line: str) -> None:
        """Parse a single mailmap line into internal lookup tables."""
        emails = _EMAIL_RE.findall(line)
        if not emails:
            return

        # _EMAIL_RE.split alternates: [text, email, text, email, ...]
        parts = _EMAIL_RE.split(line)

        if len(emails) == 1:
            # "Proper Name <proper@email>" — canonical name for this email
            proper_name = parts[0].strip()
            proper_email = emails[0].lower()
            if proper_name:
                self._by_email[proper_email] = (proper_name, proper_email)

        elif len(emails) == 2:
            proper_email = emails[0].lower()
            commit_email = emails[1].lower()
            proper_name = parts[0].strip()
            commit_name = parts[2].strip() if len(parts) > 2 else ""

            if proper_name and commit_name:
                # "Proper <proper> Commit <commit>" — match on both name+email
                self._by_name_email[(commit_name, commit_email)] = (
                    proper_name,
                    proper_email,
                )
            elif proper_name:
                # "Proper <proper> <commit>" — map commit email to proper name+email
                self._by_email[commit_email] = (proper_name, proper_email)
            else:
                # "<proper> <commit>" — map commit email to proper email, keep name
                self._by_email[commit_email] = ("", proper_email)

    def resolve(self, name: str, email: str) -> tuple[str, str]:
        """Resolve a name/email pair to canonical form."""
        email_lower = email.lower()

        # Exact name+email match takes priority
        result = self._by_name_email.get((name, email_lower))
        if result:
            return result

        # Email-only match
        result = self._by_email.get(email_lower)
        if result:
            proper_name, proper_email = result
            return (proper_name or name, proper_email)

        return (name, email_lower)


class EmailNormalizer:
    """Fallback normalizer: groups by email, uses most recent author name as canonical."""

    def __init__(self) -> None:
        self._latest: dict[str, tuple[str, str]] = {}

    def observe(self, name: str, email: str, date: str) -> None:
        """Record a name/email/date observation."""
        email_lower = email.lower()
        existing = self._latest.get(email_lower)
        if existing is None or date > existing[1]:
            self._latest[email_lower] = (name, date)

    def resolve(self, name: str, email: str) -> tuple[str, str]:
        """Resolve using most-recent-name-per-email."""
        email_lower = email.lower()
        entry = self._latest.get(email_lower)
        if entry:
            return (entry[0], email_lower)
        return (name, email_lower)
