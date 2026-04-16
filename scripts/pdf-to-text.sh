#!/usr/bin/env bash
# Wrap `pdftotext -layout` so the extraction pipeline stays inside a
# repo-local script path and does not need a fresh top-level Bash
# permission rule for every PDF pulled into tmp/.
#
# Usage:
#   scripts/pdf-to-text.sh <input.pdf> [<output.txt>]
#
# If output is omitted, writes next to the input with a .txt extension.
# Extra args after output are forwarded to pdftotext, so callers can
# append flags like -f 1 -l 3 to extract specific page ranges.

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <input.pdf> [<output.txt>] [pdftotext-args...]" >&2
  exit 2
fi

input="$1"
shift

if [[ ! -f "$input" ]]; then
  echo "error: input not found: $input" >&2
  exit 1
fi

if [[ $# -ge 1 && "$1" != -* ]]; then
  output="$1"
  shift
else
  output="${input%.pdf}.txt"
fi

pdftotext -layout "$@" "$input" "$output"
wc -l "$output"
