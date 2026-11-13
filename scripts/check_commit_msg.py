"""commit-msg hook: enforce Conventional Commit subjects.

Example: `feat(api): add health check`
"""

import re
import sys

PATTERN = re.compile(
    r"^(feat|fix|perf|test|docs|chore|refactor|style|ci|build|revert)"
    r"(\([a-z0-9_./-]+\))?!?: \S.{2,}$"
)
SKIP_PREFIXES = ("Merge ", "Revert ", "fixup! ", "squash! ")
MAX_LEN = 72


def main(path: str) -> int:
    with open(path, encoding="utf-8") as f:
        subject = f.readline().rstrip("\n")
    if subject.startswith(SKIP_PREFIXES):
        return 0
    if len(subject) > MAX_LEN:
        print(f"commit subject is {len(subject)} chars; keep it <= {MAX_LEN}")
        return 1
    if not PATTERN.match(subject):
        print("commit subject must look like: type(scope): description")
        print("types: feat fix perf test docs chore refactor style ci build revert")
        print(f"got: {subject!r}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
