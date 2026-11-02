#!/bin/sh
# Pure identity check: --version exits before loading config or probing quotas.
set -eu
output=$("$1" --version)
set -- $output
[ "$#" -eq 3 ] && [ "$1" = pacelever ]
printf '%s\n' "$3"
