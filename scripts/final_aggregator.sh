#!/bin/bash
# final_aggregator.sh <JOB_ID>
#
# 1) Merge logs/job_<JOBID>_rank_*.log in subrange order => logs/final_<JOBID>_aggregated.log
# 2) Sum prime counts => TOTAL_PRIMES
# 3) Sort all primes => results/final_<JOBID>_primes_only.txt
# 4) HPC Elapsed from sacct => stored in results/final_<JOBID>.txt

JOBID="$1"
if [ -z "$JOBID" ]; then
  echo "Usage: $0 <JOB_ID>"
  exit 1
fi

LOGS_DIR="/tn/5Net/raid-gold/private-git-code/hpc/logs"
RESULTS_DIR="/tn/5Net/raid-gold/private-git-code/hpc/results"

RANK_LOGS=( "$LOGS_DIR"/job_"$JOBID"_rank_*.log )
if [ ! -f "${RANK_LOGS[0]}" ]; then
  echo "No partial logs found for job $JOBID"
  exit 0
fi

TMPFILE_SORT=$(mktemp)
ALL_PRIMES_TMP=$(mktemp)
TOTAL_PRIMES=0

for f in "${RANK_LOGS[@]}"; do
  # parse line with subrange=..., found=NN
  line=$(grep -m1 "subrange=" "$f")
  s=$(echo "$line" | sed -E 's/.*subrange=\[([0-9]+)\.\.[0-9]+\].*/\1/')
  e=$(echo "$line" | sed -E 's/.*subrange=\[[0-9]+\.\.([0-9]+)\].*/\1/')
  n=$(echo "$line" | sed -E 's/.*found=([0-9]+).*/\1/')
  [[ "$s" =~ ^[0-9]+$ ]] || s=0
  [[ "$e" =~ ^[0-9]+$ ]] || e=0
  [[ "$n" =~ ^[0-9]+$ ]] || n=0
  TOTAL_PRIMES=$((TOTAL_PRIMES + n))

  echo -e "${s}\t${e}\t${n}\t${f}\t${line}" >> "$TMPFILE_SORT"

  # parse primes
  primes_line=$(grep "Primes=" "$f")
  prime_list=$(echo "$primes_line" | sed -E 's/.*Primes=\[([^]]*)\].*/\1/')
  echo "$prime_list" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
    | grep -E '^[0-9]+$' >> "$ALL_PRIMES_TMP"
done

# Sort subrange => logs/final_<JOBID>_aggregated.log
SORTED=$(sort -k1,1n "$TMPFILE_SORT")
AGG_LOG="$LOGS_DIR/final_${JOBID}_aggregated.log"
rm -f "$AGG_LOG"
while IFS=$'\t' read -r start_val end_val found_val fname line; do
  echo "=== $fname ===" >> "$AGG_LOG"
  echo "$line" >> "$AGG_LOG"
  grep "Primes=" "$fname" >> "$AGG_LOG"
  echo >> "$AGG_LOG"
done <<< "$SORTED"

# Sort all primes => results/final_<JOBID>_primes_only.txt
mkdir -p "$RESULTS_DIR"
ALL_PRIMES_SORTED="$RESULTS_DIR/final_${JOBID}_primes_only.txt"
sort -n "$ALL_PRIMES_TMP" | uniq > "$ALL_PRIMES_SORTED"

# HPC elapsed from sacct
if which sacct >/dev/null 2>&1; then
  ELAPSED=$(sacct -j "$JOBID" --format=Elapsed --noheader -P 2>/dev/null | head -n1)
else
  ELAPSED="UNKNOWN"
fi
[ -z "$ELAPSED" ] && ELAPSED="UNKNOWN"

# final_{JOBID}.txt
FINAL_RES="$RESULTS_DIR/final_${JOBID}.txt"
{
  echo "=== HPC Job $JOBID Final Report ==="
  echo "Total HPC Elapsed: $ELAPSED"
  echo "Sum of prime counts: $TOTAL_PRIMES"
  echo
  echo "Check $AGG_LOG for subrange-sorted partial logs."
  echo "Check $ALL_PRIMES_SORTED for global prime list."
} > "$FINAL_RES"

rm -f "$TMPFILE_SORT" "$ALL_PRIMES_TMP"

echo "Aggregator wrote final report: $FINAL_RES"
echo "Also created aggregated log => $AGG_LOG"
echo "Master prime list => $ALL_PRIMES_SORTED"
echo "HPC Elapsed: $ELAPSED"

