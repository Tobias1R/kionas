
#!/bin/bash
DELETE_FILES=false

# read arg --delete-files
while [[ "$1" == --* ]]; do
  case "$1" in
    --delete-files)
      DELETE_FILES=true
      shift
      ;;
    *)      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# for each file in _dev/benchmark/generated_fake_data_b*.sql, run it against the database and measure the time taken
for file in _dev/benchmark/generated_fake_data_b*.sql
do
  echo "Running $file"
  start_time=$(date +%s)
  ./docker-target/debug/client --username kionas --password kionas --query-file $file
    end_time=$(date +%s)
    echo "Time taken: $((end_time - start_time)) seconds"
    # if [ "$DELETE_FILES" = true ]; then
    #     rm "$file"
    # fi
done