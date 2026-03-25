set -euo pipefail
cd /workspace

out="roadmaps/SILK_ROAD/DATAFUSION/plans/execution-ep3-postchange-capture.csv"
batch_sizes_csv="8192"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --batch-sizes)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --batch-sizes (example: --batch-sizes 1024,4096,8192,16384)" >&2
        exit 1
      fi
      batch_sizes_csv="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

IFS=',' read -r -a batch_sizes <<< "$batch_sizes_csv"
if [[ ${#batch_sizes[@]} -eq 0 ]]; then
  echo "--batch-sizes produced no values" >&2
  exit 1
fi

declare -A measured_rps_sum
declare -A measured_rps_count

mkdir -p "$(dirname "$out")"

printf '%s\n' 'run_id,timestamp_utc,profile_id,dataset_tier,row_count_per_table,table_count,warmup_runs,measured_runs,batch_size,wall_time_ms,rows_processed,rows_per_second,artifact_size_bytes,stage_runtime_ms,batch_count,result_row_count,notes' > "$out"

cases=(
  "S,SCAN,scan_only,scripts/benchmarks/ep3/baseline/tier_s_scan_only.sql,10000,3"
  "S,FSORT,filter_sort,scripts/benchmarks/ep3/baseline/tier_s_filter_sort.sql,10000,3"
  "S,JOIN,join_heavy,scripts/benchmarks/ep3/baseline/tier_s_join_heavy.sql,10000,3"
  "S,AGG,aggregate_heavy,scripts/benchmarks/ep3/baseline/tier_s_aggregate_heavy.sql,10000,3"
  "M,SCAN,scan_only,scripts/benchmarks/ep3/baseline/tier_m_scan_only.sql,100000,3"
  "M,FSORT,filter_sort,scripts/benchmarks/ep3/baseline/tier_m_filter_sort.sql,100000,3"
  "M,JOIN,join_heavy,scripts/benchmarks/ep3/baseline/tier_m_join_heavy.sql,100000,3"
  "M,AGG,aggregate_heavy,scripts/benchmarks/ep3/baseline/tier_m_aggregate_heavy.sql,100000,3"
)

for batch_size in "${batch_sizes[@]}"; do
  for c in "${cases[@]}"; do
    IFS=',' read -r tier pcode pid qfile rowcount tablecount <<< "$c"

    for i in 1 2 3 4; do
      if [ "$i" -eq 1 ]; then
        phase="WARM"
        seq="01"
        phase_note="warmup"
      else
        phase="MEAS"
        seq=$(printf '%02d' "$((i - 1))")
        phase_note="measured"
      fi

      run_id="EP3-PC-${tier}-${pcode}-${phase}-${seq}-B${batch_size}"
      ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

      output=$(KIONAS_BENCH_BATCH_SIZE_ROWS="$batch_size" docker-target/debug/client --username kionas --password kionas --query-file "$qfile" 2>&1)

      wall_ms=$(printf '%s\n' "$output" | sed -n 's/.*statement 2: SUCCESS (\([0-9][0-9]*\) ms).*/\1/p' | tail -n1)
      rows=$(printf '%s\n' "$output" | sed -n 's/.*rows=\([0-9][0-9]*\) columns=[0-9][0-9]* batches=\([0-9][0-9]*\).*/\1/p' | tail -n1)
      batches=$(printf '%s\n' "$output" | sed -n 's/.*rows=[0-9][0-9]* columns=[0-9][0-9]* batches=\([0-9][0-9]*\).*/\1/p' | tail -n1)
      summary_ms=$(printf '%s\n' "$output" | sed -n 's/.*total=[0-9][0-9]* success=[0-9][0-9]* failed=[0-9][0-9]* (\([0-9][0-9]*\) ms).*/\1/p' | tail -n1)
      stage_runtime_ms=$(printf '%s\n' "$output" | sed -n 's/^Stage [0-9][0-9]*: .* = \([0-9][0-9]*\)ms total.*/\1/p' | awk '{s+=$1} END{if (NR>0) print s}')
      network_bytes_total=$(printf '%s\n' "$output" | sed -n 's/^Network transfer totals: write_ms=[0-9][0-9]* batches=[0-9][0-9]* bytes=\([0-9][0-9]*\)$/\1/p' | tail -n1)

      if [ -z "$wall_ms" ] || [ -z "$rows" ] || [ -z "$batches" ]; then
        echo "Failed to parse output for $run_id" >&2
        echo "$output" >&2
        exit 1
      fi

      rps=$(awk -v r="$rows" -v w="$wall_ms" 'BEGIN { printf "%.2f", (r * 1000.0) / w }')
      note="$qfile $phase_note; batch_size_rows=$batch_size; statement 2 SUCCESS ($wall_ms ms); execution summary $summary_ms ms"
      stage_runtime_value=""
      artifact_size_value=""
      if [ -n "$stage_runtime_ms" ]; then
        stage_runtime_value="$stage_runtime_ms"
      fi
      if [ -n "$network_bytes_total" ]; then
        artifact_size_value="$network_bytes_total"
      fi

      if [ "$phase" = "MEAS" ]; then
        current_sum="${measured_rps_sum[$batch_size]:-0}"
        measured_rps_sum[$batch_size]=$(awk -v a="$current_sum" -v b="$rps" 'BEGIN { printf "%.6f", a + b }')
        measured_rps_count[$batch_size]=$(( ${measured_rps_count[$batch_size]:-0} + 1 ))
      fi

      printf '%s,%s,%s,%s,%s,%s,1,3,%s,%s,%s,%s,%s,%s,%s,%s,"%s"\n' \
        "$run_id" "$ts" "$pid" "$tier" "$rowcount" "$tablecount" "$batch_size" "$wall_ms" "$rows" "$rps" "$artifact_size_value" "$stage_runtime_value" "$batches" "$rows" "$note" >> "$out"

      echo "$run_id batch_size=${batch_size} wall=${wall_ms}ms rows=${rows} batches=${batches} network_bytes=${artifact_size_value:-n/a}"
    done
  done
done

echo "WROTE $out"

best_batch_size=""
best_avg_rps="0"

for batch_size in "${batch_sizes[@]}"; do
  count="${measured_rps_count[$batch_size]:-0}"
  if [ "$count" -eq 0 ]; then
    continue
  fi

  sum="${measured_rps_sum[$batch_size]:-0}"
  avg=$(awk -v s="$sum" -v c="$count" 'BEGIN { if (c > 0) printf "%.6f", s / c; else print "0" }')

  if [ -z "$best_batch_size" ] || awk -v a="$avg" -v b="$best_avg_rps" 'BEGIN { exit !(a > b) }'; then
    best_batch_size="$batch_size"
    best_avg_rps="$avg"
  fi
done

if [ -n "$best_batch_size" ]; then
  reference_batch_size="4096"
  reference_count="${measured_rps_count[$reference_batch_size]:-0}"

  if [ "$reference_count" -eq 0 ]; then
    reference_batch_size=""
    reference_avg_rps="0"
    for batch_size in "${batch_sizes[@]}"; do
      if [ "$batch_size" = "$best_batch_size" ]; then
        continue
      fi
      count="${measured_rps_count[$batch_size]:-0}"
      if [ "$count" -eq 0 ]; then
        continue
      fi
      sum="${measured_rps_sum[$batch_size]:-0}"
      avg=$(awk -v s="$sum" -v c="$count" 'BEGIN { if (c > 0) printf "%.6f", s / c; else print "0" }')
      if [ -z "$reference_batch_size" ] || awk -v a="$avg" -v b="$reference_avg_rps" 'BEGIN { exit !(a > b) }'; then
        reference_batch_size="$batch_size"
        reference_avg_rps="$avg"
      fi
    done
  else
    reference_sum="${measured_rps_sum[$reference_batch_size]:-0}"
    reference_avg_rps=$(awk -v s="$reference_sum" -v c="$reference_count" 'BEGIN { if (c > 0) printf "%.6f", s / c; else print "0" }')
  fi

  if [ -n "${reference_batch_size:-}" ] && awk -v r="${reference_avg_rps:-0}" 'BEGIN { exit !(r > 0) }'; then
    improvement_pct=$(awk -v b="$best_avg_rps" -v r="$reference_avg_rps" 'BEGIN { printf "%.1f", ((b - r) / r) * 100 }')
    echo "Optimal batch size: ${best_batch_size} rows (${improvement_pct}% better throughput than ${reference_batch_size})"
  else
    echo "Optimal batch size: ${best_batch_size} rows"
  fi
fi