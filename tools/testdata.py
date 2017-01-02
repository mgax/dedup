# Generate a stresstest input script. Each file is a snapshot of the given Git
# repository, as produced by `git archive`.

import sys
import subprocess

[repo] = sys.argv[1:]

out = subprocess.check_output('git log --oneline', cwd=repo, shell=True)
hashes = [line.decode('latin1').split()[0] for line in out.splitlines()]
hashes.reverse()

line = '{:03d}-{}: git --git-dir={}/.git archive {}^{{tree}}'
for n, h in enumerate(hashes):
    print(line.format(n+1, h, repo, h))
