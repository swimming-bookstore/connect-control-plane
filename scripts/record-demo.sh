#!/usr/bin/env bash
exec python3 "$(cd "$(dirname "$0")" && pwd)/record-demo.py" "$@"
