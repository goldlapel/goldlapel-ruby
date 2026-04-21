#!/usr/bin/env bash
# Run the Ruby test suite and surface a skip-count summary at the end.
#
# Integration tests in test/test_async_native.rb (and a few others) skip
# cleanly when Postgres isn't reachable. That's valid behavior — we don't
# want to fail tests just because a developer doesn't have Postgres running
# locally — but minitest's default summary line is easy to miss, so a
# developer might reasonably assume "all tests passed" when integration
# coverage never ran.
#
# This wrapper runs rake test, captures its output (while still streaming
# it to the terminal in real time), then parses the trailing minitest
# summary for the skip count and prints a highlighted reminder if any
# tests skipped.
#
# Exit code is preserved from rake.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Run rake test, tee output to stderr for live streaming, capture full
# output for post-processing. PIPESTATUS[0] preserves rake's exit code.
output=$(bundle exec rake test "$@" 2>&1 | tee /dev/stderr; exit "${PIPESTATUS[0]}")
rc=$?

# Parse skip count from minitest summary line:
#   "Finished in 1.23s, 45 runs, 67 assertions, 0 failures, 0 errors, 8 skips"
# grep -oE isolates the "N skips" token; a second grep extracts the int.
# Default to 0 if no match.
skipped=$(printf '%s\n' "$output" | grep -oE '[0-9]+ skips' | head -1 | grep -oE '[0-9]+' || true)
skipped=${skipped:-0}

# Only show the summary banner for successful runs with skips. On failure,
# rake/minitest's own output is what the developer needs to see.
if [ "$rc" -eq 0 ] && [ "$skipped" -gt 0 ]; then
    printf '\n\033[33m==========================================\033[0m\n'
    printf '\033[33m⚠  %d tests skipped\033[0m — integration tests require a local Postgres.\n' "$skipped"
    printf '   Set PGHOST / PGUSER / PGPASSWORD and re-run, or rely on the CI workflow\n'
    printf '   (.github/workflows/test.yml) which provisions postgres:16 automatically.\n'
    printf '\033[33m==========================================\033[0m\n\n'
fi

exit "$rc"
