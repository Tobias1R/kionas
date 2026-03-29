
#!/bin/bash

# 200 files per batch max
MAX_ROWS_PER_BATCH=25000
# total batches
TOTAL_BATCHES=200
# ROWS
ROWS=5000000
# database name
DATABASE=bench2512

# loop to generate batches
for ((i=1; i<=TOTAL_BATCHES; i++))
do
  START_IDX=$(( (i - 1) * MAX_ROWS_PER_BATCH + 1 ))
  END_IDX=$(( i * MAX_ROWS_PER_BATCH ))
  if [ $END_IDX -gt $ROWS ]; then
    END_IDX=$ROWS
  fi
  echo "Generating batch $i: rows $START_IDX to $END_IDX"
  python3 scripts/generate_fake_data_sql.py --output "_dev/benchmark/generated_fake_data_b${i}.sql" --warehouse compute_xl --database $DATABASE --schema seed1 --rows $((END_IDX - START_IDX + 1)) --batch-size 2000 --seed $((42 + i)) --initial-idx $START_IDX
done