#!/usr/bin/env bash
set -euo pipefail

label=${1:?usage: sccache-summary.sh LABEL}
summary_file=${GITHUB_STEP_SUMMARY:-/dev/null}
finished_at=$(date +%s)

if stats=$(sccache --show-stats 2>&1); then
  :
else
  stats="sccache statistics unavailable:
$stats"
fi

# Keep the statistics in the step log as well as the durable workflow summary.
printf '%s\n' "$stats"

{
  printf '### %s workload\n\n' "$label"
  if [[ ${CI_TIMED_WORK_STARTED_AT:-} =~ ^[0-9]+$ ]]; then
    elapsed=$((finished_at - CI_TIMED_WORK_STARTED_AT))
    printf -- '- Timed workload: %dm %02ds\n\n' "$((elapsed / 60))" "$((elapsed % 60))"
  else
    printf '%s\n\n' '- Timed workload: unavailable (timer step did not complete)'
  fi
  printf '%s\n' '```text'
  printf '%s\n' "$stats"
  printf '%s\n' '```'
} >>"$summary_file"
