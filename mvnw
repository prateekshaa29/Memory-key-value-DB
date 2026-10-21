#!/bin/sh
# Maven wrapper runner for Unix/Linux/macOS
if command -v mvn >/dev/null 2>&1; then
    exec mvn "$@"
fi
echo "[ERROR] Maven is not installed on PATH. Please install Maven." >&2
exit 1
