#!/bin/bash
set -x
source /tn/5Net/raid-gold/virtual-python-dirs/hpc-public/bin/activate

LOGS_DIR="/tn/5Net/raid-gold/private-git-code/hpc/logs"
RESULTS_DIR="/tn/5Net/raid-gold/private-git-code/hpc/results"

echo "DEBUG: RANGE_START=$RANGE_START"
echo "DEBUG: RANGE_END=$RANGE_END"
echo "DEBUG: RUN_ID=$RUN_ID"

echo "Multi-node HPC job started on partition=$SLURM_JOB_PARTITION, job_id=$SLURM_JOB_ID"
echo "Nodes=$SLURM_JOB_NUM_NODES, tasks=$SLURM_NTASKS"
echo "Range=[$RANGE_START..$RANGE_END], run_id=$RUN_ID"

# Actually do concurrency:
srun -n "$SLURM_NTASKS" \
  python /tn/5Net/raid-gold/private-git-code/hpc/modules/multi_node_prime_finder.py \
    "$RANGE_START" "$RANGE_END" "$RUN_ID" "$LOGS_DIR"

# Optionally check for partial logs:
if compgen -G "$LOGS_DIR/job_${SLURM_JOB_ID}_rank_*.log" > /dev/null; then
  echo "Partial rank logs found in $LOGS_DIR"
else
  echo "No partial rank logs found in $LOGS_DIR."
fi

echo "✅ HPC job finished. Logs in $LOGS_DIR, final results in $RESULTS_DIR/final_${SLURM_JOB_ID}.txt"

