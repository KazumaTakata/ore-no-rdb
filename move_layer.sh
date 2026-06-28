#!/usr/bin/env bash
# Usage: move_layer.sh <layer> <module1> <module2> ...
# Moves src/<moduleN>.rs -> src/<layer>/<moduleN>.rs, wires up the layer module,
# and rewrites all import paths (both `crate::mod::` and grouped ` mod::` forms)
# to `crate::<layer>::mod::` / ` <layer>::mod::`.
set -euo pipefail
cd "$(dirname "$0")"

layer="$1"; shift
mods=("$@")

mkdir -p "src/$layer"

# 1. git mv each module into the layer dir
for m in "${mods[@]}"; do
  git mv "src/$m.rs" "src/$layer/$m.rs"
done

# 2. ensure layer module file exists and declares each module
touch "src/$layer.rs"
for m in "${mods[@]}"; do
  if ! grep -q "^pub mod $m;$" "src/$layer.rs"; then
    echo "pub mod $m;" >> "src/$layer.rs"
  fi
done
# keep it sorted/clean
sort -u -o "src/$layer.rs" "src/$layer.rs"

# 3. register `mod <layer>;` in main.rs if not present
if ! grep -q "^mod $layer;$" src/main.rs; then
  # insert after the last `^mod ` line
  perl -0pi -e "s/(^mod [a-z_0-9]+;\n)(?!mod )/\$1mod $layer;\n/m" src/main.rs || true
  grep -q "^mod $layer;$" src/main.rs || echo "mod $layer;" >> src/main.rs
fi

# 4. remove the now-moved `mod <moduleN>;` lines from main.rs
for m in "${mods[@]}"; do
  perl -i -ne "print unless /^mod $m;\$/" src/main.rs
done

# 5. rewrite import paths across all src files (except the moved files themselves)
for m in "${mods[@]}"; do
  for f in $(grep -rl "$m::" src/ --include="*.rs" | grep -v "src/$layer/$m.rs"); do
    # direct form: crate::m:: -> crate::layer::m::
    perl -i -pe "s{crate::$m\::}{crate::$layer\::$m\::}g" "$f"
    # grouped/bare form: m:: -> layer::m:: (skip if already prefixed by ::)
    perl -i -pe "s{(?<!::)(?<![_\\w])$m\::}{$layer\::$m\::}g" "$f"
  done
done

echo "moved layer '$layer': ${mods[*]}"
