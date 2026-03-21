"""Tests for .mailmap parsing (all 4 formats)."""

from __future__ import annotations

import tempfile
from pathlib import Path

from repostats.mailmap import Mailmap


def _create_mailmap(content: str) -> Mailmap:
    """Create a Mailmap from a string, writing to a temp file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".mailmap", delete=False) as f:
        f.write(content)
        f.flush()
        return Mailmap.from_file(f.name)


def test_format_name_email() -> None:
    """Format: Proper Name <proper@email>."""
    mm = _create_mailmap("Alice Smith <alice@canonical.com>\n")
    name, email = mm.resolve("alice", "alice@canonical.com")
    assert name == "Alice Smith"
    assert email == "alice@canonical.com"


def test_format_email_to_email() -> None:
    """Format: <proper@email> <commit@email>."""
    mm = _create_mailmap("<proper@ex.com> <old@ex.com>\n")
    name, email = mm.resolve("Whatever", "old@ex.com")
    # Name should be preserved, email mapped
    assert name == "Whatever"
    assert email == "proper@ex.com"


def test_format_name_email_to_email() -> None:
    """Format: Proper Name <proper@email> <commit@email>."""
    mm = _create_mailmap("Alice <alice@new.com> <alice@old.com>\n")
    name, email = mm.resolve("old-alice", "alice@old.com")
    assert name == "Alice"
    assert email == "alice@new.com"


def test_format_name_email_to_name_email() -> None:
    """Format: Proper Name <proper@email> Commit Name <commit@email>."""
    mm = _create_mailmap("Alice Smith <alice@new.com> alice-old <alice@old.com>\n")
    name, email = mm.resolve("alice-old", "alice@old.com")
    assert name == "Alice Smith"
    assert email == "alice@new.com"


def test_name_email_match_takes_priority() -> None:
    """Exact name+email match overrides email-only match."""
    content = (
        "General Alice <alice@new.com> <alice@old.com>\n"
        "Specific Alice <alice@specific.com> alice-bot <alice@old.com>\n"
    )
    mm = _create_mailmap(content)

    # Name+email match should win
    name, email = mm.resolve("alice-bot", "alice@old.com")
    assert name == "Specific Alice"
    assert email == "alice@specific.com"

    # Email-only match for different name
    name, email = mm.resolve("someone-else", "alice@old.com")
    assert name == "General Alice"
    assert email == "alice@new.com"


def test_case_insensitive_email() -> None:
    """Email matching is case-insensitive."""
    mm = _create_mailmap("Alice <alice@ex.com> <ALICE@EX.COM>\n")
    name, email = mm.resolve("whatever", "Alice@Ex.Com")
    assert name == "Alice"
    assert email == "alice@ex.com"


def test_comments_and_blank_lines() -> None:
    """Comments and blank lines are skipped."""
    content = "# This is a comment\n\nAlice <alice@ex.com>\n"
    mm = _create_mailmap(content)
    name, email = mm.resolve("old", "alice@ex.com")
    assert name == "Alice"


def test_no_match_returns_original() -> None:
    """Unknown author/email returns originals (with lowercase email)."""
    mm = _create_mailmap("Alice <alice@ex.com>\n")
    name, email = mm.resolve("Bob", "bob@other.com")
    assert name == "Bob"
    assert email == "bob@other.com"


def test_missing_file_returns_empty_mailmap() -> None:
    """Non-existent file returns an empty Mailmap (no crash)."""
    mm = Mailmap.from_file("/nonexistent/path/.mailmap")
    name, email = mm.resolve("Alice", "alice@ex.com")
    assert name == "Alice"
    assert email == "alice@ex.com"
