#!/usr/bin/env bash
set -euo pipefail

mkdir -p bin
cat > bin/process.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
INPUT="${1:?usage: process.sh <input>}"
cat "$INPUT" > output.txt

sleep 10s
EOF
chmod +x bin/process.sh
