from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlparse

MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


def test_relative_readme_links_resolve():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    missing: list[str] = []

    for raw_target in MARKDOWN_LINK.findall(readme):
        target = raw_target.strip().strip("<>").split("#", 1)[0]
        if not target or target.startswith("#"):
            continue
        parsed = urlparse(target)
        if parsed.scheme or target.startswith("//"):
            continue
        candidate = root / unquote(target)
        if not candidate.exists():
            missing.append(raw_target)

    assert not missing, f"README contains missing relative links: {sorted(set(missing))}"
