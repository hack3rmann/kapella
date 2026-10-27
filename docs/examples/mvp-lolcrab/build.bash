#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d src/.git ]]; then
    rm -rf src
    git clone --depth 1 https://github.com/mazznoer/lolcrab.git src
fi

cargo install --path src --root "$PWD" --locked --force
