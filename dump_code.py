import glob
import os

root = r'D:\Studies\Epita University\2nd Semester\DSA\Project\EPITA-DSP-practical-work'
patterns = ['**/*.py', '**/*.yml', '**/*.yaml', '**/Dockerfile', '**/.env.template']
paths = []
for pat in patterns:
    paths.extend(glob.glob(os.path.join(root, pat), recursive=True))
paths = sorted(set(paths))
out_path = os.path.join(root, 'all_code_dump.txt')
with open(out_path, 'w', encoding='utf-8') as f:
    for p in paths:
        rel = os.path.relpath(p, root)
        f.write(f"===== {rel} =====\n")
        try:
            with open(p, 'r', encoding='utf-8') as r:
                f.write(r.read())
        except Exception as e:
            f.write(f"<ERROR reading file: {e}>\n")
        f.write("\n\n")

print(f'Wrote {len(paths)} files to {out_path}')
