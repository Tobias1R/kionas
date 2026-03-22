set -euo pipefail
cd /workspace

out="roadmaps/SILK_ROAD/DATAFUSION/plans/execution-ep3-postchange-capture.csv"
mkdir -p "$(dirname "$out")"

printf '%s\n' 'run_id,timestamp_utc,profile_id,dataset_tier,row_count_per_table,table_count,warmup_runs,measured_runs,wall_time_ms,rows_processed,rows_per_second,artifact_size_bytes,stage_runtime_ms,batch_count,result_row_count,notes' > "$out"

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

    run_id="EP3-PC-${tier}-${pcode}-${phase}-${seq}"
    ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    output=$(docker-target/debug/client --username kionas --password kionas --query-file "$qfile" 2>&1)

    wall_ms=$(printf '%s\n' "$output" | sed -n 's/.*statement 2: SUCCESS (\([0-9][0-9]*\) ms).*/\1/p' | tail -n1)
    rows=$(printf '%s\n' "$output" | sed -n 's/.*rows=\([0-9][0-9]*\) columns=[0-9][0-9]* batches=\([0-9][0-9]*\).*/\1/p' | tail -n1)
    batches=$(printf '%s\n' "$output" | sed -n 's/.*rows=[0-9][0-9]* columns=[0-9][0-9]* batches=\([0-9][0-9]*\).*/\1/p' | tail -n1)
    summary_ms=$(printf '%s\n' "$output" | sed -n 's/.*total=[0-9][0-9]* success=[0-9][0-9]* failed=[0-9][0-9]* (\([0-9][0-9]*\) ms).*/\1/p' | tail -n1)

    if [ -z "$wall_ms" ] || [ -z "$rows" ] || [ -z "$batches" ]; then
      echo "Failed to parse output for $run_id" >&2
      echo "$output" >&2
      exit 1
    fi

    rps=$(awk -v r="$rows" -v w="$wall_ms" 'BEGIN { printf "%.2f", (r * 1000.0) / w }')
    note="$qfile $phase_note; statement 2 SUCCESS ($wall_ms ms); execution summary $summary_ms ms"

    printf '%s,%s,%s,%s,%s,%s,1,3,%s,%s,%s,,,%s,%s,"%s"\n' \
      "$run_id" "$ts" "$pid" "$tier" "$rowcount" "$tablecount" "$wall_ms" "$rows" "$rps" "$batches" "$rows" "$note" >> "$out"

    echo "$run_id wall=${wall_ms}ms rows=${rows} batches=${batches}"
  done
done

echo "WROTE $out"