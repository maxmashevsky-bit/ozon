#!/usr/bin/env python3
"""Check byte-for-byte reproducibility, including from an uncommitted worktree."""
import hashlib
from stack import ROOT, GO_ENV, command
paths = [ROOT/'internal/graph/generated.go', ROOT/'internal/graph/model/models_gen.go', *sorted((ROOT/'internal/store/postgres/db').glob('*.go')), ROOT/'internal/graph/schema.resolvers.go']
def digest():
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
before = digest()
command(['go','generate','./...'],env=GO_ENV)
after = digest()
changed = [p for p in before if before[p] != after[p]]
if changed:
    raise SystemExit('Generation changed files: ' + ', '.join(changed))
print('PASS reproducible sqlc and gqlgen generation')
