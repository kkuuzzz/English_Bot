import re

DASH_RE = re.compile(r"\s*(—|–|-)\s*")


def parse_entry(text: str) -> dict | None:
    """
    Expected:
      EN — RU | ex: ... | tag: ...
    """
    parts = [p.strip() for p in text.split("|")]
    head = parts[0]

    m = DASH_RE.split(head, maxsplit=1)
    # split returns [en, dash, ru] if matched
    if len(m) < 3:
        return None

    en = m[0].strip()
    ru = m[2].strip()
    if not en or not ru:
        return None

    example = None
    tags = None

    for p in parts[1:]:
        pl = p.lower()
        if pl.startswith("ex:") or pl.startswith("example:"):
            example = p.split(":", 1)[1].strip()
        elif pl.startswith("tag:") or pl.startswith("tags:"):
            tags = p.split(":", 1)[1].strip()

    return {"en": en, "ru": ru, "example": example, "tags": tags}
