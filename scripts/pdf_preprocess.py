"""Transform Markdown pipe tables into wrapped bullets for the release PDF.

GitHub retains readable tables in Markdown. XeLaTeX uses a temporary rendering
source so long technical descriptions do not overlap columns on a printed page.
"""
from __future__ import annotations

import argparse
from pathlib import Path
def is_separator(line: str) -> bool:
    """Check Markdown table separators in linear time without regex backtracking."""
    if not line.lstrip().startswith("|"):
        return False
    parts = [part.strip() for part in line.strip().strip("|").split("|")]
    if len(parts) < 2:
        return False
    for part in parts:
        value = part.removeprefix(":").removesuffix(":")
        if len(value) < 3 or any(char != "-" for char in value):
            return False
    return True


def cells(line: str) -> list[str]:
    return [part.strip() for part in line.strip().strip('|').split('|')]


def prepare(markdown: str) -> str:
    lines = markdown.splitlines()
    out = []
    i = 0
    fenced = False
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith('```'):
            fenced = not fenced
            out.append(line)
            i += 1
            continue
        if (not fenced and line.lstrip().startswith('|')
                and i + 1 < len(lines) and is_separator(lines[i + 1])):
            labels = cells(line)
            i += 2
            out.append('')
            while i < len(lines) and lines[i].lstrip().startswith('|'):
                row = cells(lines[i])
                i += 1
                if len(row) != len(labels):
                    out.append(' | '.join(row))
                    continue
                detail = '; '.join(
                    f'**{header}:** {value}' for header, value in zip(labels[1:], row[1:])
                )
                out.append(f'- **{row[0]}.** {detail}')
                out.append('')
            continue
        out.append(line)
        i += 1
    return '\n'.join(out) + '\n'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    args.destination.parent.mkdir(parents=True, exist_ok=True)
    args.destination.write_text(
        prepare(args.source.read_text(encoding='utf-8')), encoding='utf-8'
    )


if __name__ == '__main__':
    main()
