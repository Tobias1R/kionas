# Details

Date : 2026-03-27 14:35:18

Directory c:\\code\\kionas\\worker

Total : 75 files,  17593 codes, 2518 comments, 2112 blanks, all 22223 lines

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [worker/Cargo.toml](/worker/Cargo.toml) | TOML | 49 | 6 | 7 | 62 |
| [worker/build.rs](/worker/build.rs) | Rust | 13 | 0 | 1 | 14 |
| [worker/src/authz.rs](/worker/src/authz.rs) | Rust | 255 | 111 | 37 | 403 |
| [worker/src/execution/aggregate/aggregate\_avg.rs](/worker/src/execution/aggregate/aggregate_avg.rs) | Rust | 21 | 27 | 4 | 52 |
| [worker/src/execution/aggregate/aggregate\_count.rs](/worker/src/execution/aggregate/aggregate_count.rs) | Rust | 8 | 16 | 2 | 26 |
| [worker/src/execution/aggregate/aggregate\_max.rs](/worker/src/execution/aggregate/aggregate_max.rs) | Rust | 12 | 16 | 2 | 30 |
| [worker/src/execution/aggregate/aggregate\_min.rs](/worker/src/execution/aggregate/aggregate_min.rs) | Rust | 12 | 16 | 2 | 30 |
| [worker/src/execution/aggregate/aggregate\_sum.rs](/worker/src/execution/aggregate/aggregate_sum.rs) | Rust | 8 | 16 | 2 | 26 |
| [worker/src/execution/aggregate/mod.rs](/worker/src/execution/aggregate/mod.rs) | Rust | 608 | 16 | 33 | 657 |
| [worker/src/execution/artifacts.rs](/worker/src/execution/artifacts.rs) | Rust | 172 | 56 | 27 | 255 |
| [worker/src/execution/join.rs](/worker/src/execution/join.rs) | Rust | 382 | 46 | 47 | 475 |
| [worker/src/execution/mod.rs](/worker/src/execution/mod.rs) | Rust | 10 | 0 | 1 | 11 |
| [worker/src/execution/pipeline.rs](/worker/src/execution/pipeline.rs) | Rust | 1,810 | 358 | 205 | 2,373 |
| [worker/src/execution/planner.rs](/worker/src/execution/planner.rs) | Rust | 574 | 95 | 47 | 716 |
| [worker/src/execution/query.rs](/worker/src/execution/query.rs) | Rust | 531 | 157 | 72 | 760 |
| [worker/src/execution/router.rs](/worker/src/execution/router.rs) | Rust | 444 | 75 | 60 | 579 |
| [worker/src/execution/window.rs](/worker/src/execution/window.rs) | Rust | 278 | 14 | 35 | 327 |
| [worker/src/execution/window\_frame.rs](/worker/src/execution/window_frame.rs) | Rust | 64 | 22 | 10 | 96 |
| [worker/src/execution/window\_functions.rs](/worker/src/execution/window_functions.rs) | Rust | 205 | 33 | 15 | 253 |
| [worker/src/flight/client\_pool.rs](/worker/src/flight/client_pool.rs) | Rust | 129 | 64 | 19 | 212 |
| [worker/src/flight/conversion.rs](/worker/src/flight/conversion.rs) | Rust | 106 | 67 | 17 | 190 |
| [worker/src/flight/mod.rs](/worker/src/flight/mod.rs) | Rust | 3 | 0 | 1 | 4 |
| [worker/src/flight/server.rs](/worker/src/flight/server.rs) | Rust | 1,452 | 231 | 177 | 1,860 |
| [worker/src/init.rs](/worker/src/init.rs) | Rust | 91 | 10 | 18 | 119 |
| [worker/src/interops/client.rs](/worker/src/interops/client.rs) | Rust | 75 | 2 | 3 | 80 |
| [worker/src/interops/manager.rs](/worker/src/interops/manager.rs) | Rust | 55 | 0 | 6 | 61 |
| [worker/src/interops/mod.rs](/worker/src/interops/mod.rs) | Rust | 3 | 0 | 2 | 5 |
| [worker/src/lib.rs](/worker/src/lib.rs) | Rust | 13 | 0 | 3 | 16 |
| [worker/src/main.rs](/worker/src/main.rs) | Rust | 132 | 25 | 17 | 174 |
| [worker/src/services/interops\_service\_client.rs](/worker/src/services/interops_service_client.rs) | Rust | 9 | 1 | 3 | 13 |
| [worker/src/services/mod.rs](/worker/src/services/mod.rs) | Rust | 4 | 0 | 1 | 5 |
| [worker/src/services/query.rs](/worker/src/services/query.rs) | Rust | 2 | 0 | 2 | 4 |
| [worker/src/services/query\_execution.rs](/worker/src/services/query_execution.rs) | Rust | 1,535 | 274 | 196 | 2,005 |
| [worker/src/services/worker\_service\_server.rs](/worker/src/services/worker_service_server.rs) | Rust | 432 | 26 | 53 | 511 |
| [worker/src/state/mod.rs](/worker/src/state/mod.rs) | Rust | 214 | 2 | 24 | 240 |
| [worker/src/storage/deltalake.rs](/worker/src/storage/deltalake.rs) | Rust | 222 | 15 | 25 | 262 |
| [worker/src/storage/exchange.rs](/worker/src/storage/exchange.rs) | Rust | 63 | 46 | 8 | 117 |
| [worker/src/storage/minio.rs](/worker/src/storage/minio.rs) | Rust | 241 | 31 | 22 | 294 |
| [worker/src/storage/minio\_pool.rs](/worker/src/storage/minio_pool.rs) | Rust | 89 | 53 | 14 | 156 |
| [worker/src/storage/mock.rs](/worker/src/storage/mock.rs) | Rust | 59 | 0 | 8 | 67 |
| [worker/src/storage/mod.rs](/worker/src/storage/mod.rs) | Rust | 92 | 2 | 8 | 102 |
| [worker/src/storage/object\_store\_adapter.rs](/worker/src/storage/object_store_adapter.rs) | Rust | 52 | 6 | 5 | 63 |
| [worker/src/storage/object\_store\_pool.rs](/worker/src/storage/object_store_pool.rs) | Rust | 30 | 22 | 6 | 58 |
| [worker/src/storage/staging.rs](/worker/src/storage/staging.rs) | Rust | 90 | 6 | 10 | 106 |
| [worker/src/telemetry/mod.rs](/worker/src/telemetry/mod.rs) | Rust | 62 | 52 | 11 | 125 |
| [worker/src/tests/authz\_tests.rs](/worker/src/tests/authz_tests.rs) | Rust | 32 | 0 | 6 | 38 |
| [worker/src/tests/execution\_aggregate\_mod\_tests.rs](/worker/src/tests/execution_aggregate_mod_tests.rs) | Rust | 210 | 0 | 18 | 228 |
| [worker/src/tests/execution\_is\_operator\_tests.rs](/worker/src/tests/execution_is_operator_tests.rs) | Rust | 64 | 174 | 19 | 257 |
| [worker/src/tests/execution\_join\_tests.rs](/worker/src/tests/execution_join_tests.rs) | Rust | 121 | 0 | 21 | 142 |
| [worker/src/tests/execution\_pipeline\_tests.rs](/worker/src/tests/execution_pipeline_tests.rs) | Rust | 1,066 | 0 | 155 | 1,221 |
| [worker/src/tests/execution\_planner\_tests.rs](/worker/src/tests/execution_planner_tests.rs) | Rust | 172 | 0 | 22 | 194 |
| [worker/src/tests/execution\_query\_tests.rs](/worker/src/tests/execution_query_tests.rs) | Rust | 647 | 0 | 38 | 685 |
| [worker/src/tests/execution\_type\_coercion\_tests.rs](/worker/src/tests/execution_type_coercion_tests.rs) | Rust | 297 | 49 | 33 | 379 |
| [worker/src/tests/flight\_client\_pool\_tests.rs](/worker/src/tests/flight_client_pool_tests.rs) | Rust | 34 | 1 | 5 | 40 |
| [worker/src/tests/flight\_conversion\_tests.rs](/worker/src/tests/flight_conversion_tests.rs) | Rust | 147 | 7 | 29 | 183 |
| [worker/src/tests/flight\_server\_tests.rs](/worker/src/tests/flight_server_tests.rs) | Rust | 1,167 | 16 | 169 | 1,352 |
| [worker/src/tests/router\_tests.rs](/worker/src/tests/router_tests.rs) | Rust | 164 | 0 | 25 | 189 |
| [worker/src/tests/services\_query\_execution\_tests.rs](/worker/src/tests/services_query_execution_tests.rs) | Rust | 504 | 0 | 55 | 559 |
| [worker/src/tests/services\_worker\_service\_server\_tests.rs](/worker/src/tests/services_worker_service_server_tests.rs) | Rust | 114 | 0 | 17 | 131 |
| [worker/src/tests/storage\_exchange\_tests.rs](/worker/src/tests/storage_exchange_tests.rs) | Rust | 16 | 0 | 4 | 20 |
| [worker/src/tests/transactions\_maestro\_insert\_tests.rs](/worker/src/tests/transactions_maestro_insert_tests.rs) | Rust | 52 | 0 | 7 | 59 |
| [worker/src/tests/transactions\_maestro\_uri\_tests.rs](/worker/src/tests/transactions_maestro_uri_tests.rs) | Rust | 22 | 0 | 3 | 25 |
| [worker/src/tests/window\_functions\_tests.rs](/worker/src/tests/window_functions_tests.rs) | Rust | 122 | 0 | 14 | 136 |
| [worker/src/transactions/constraints/insert\_not\_null.rs](/worker/src/transactions/constraints/insert_not_null.rs) | Rust | 132 | 22 | 21 | 175 |
| [worker/src/transactions/constraints/mod.rs](/worker/src/transactions/constraints/mod.rs) | Rust | 1 | 0 | 1 | 2 |
| [worker/src/transactions/ddl/create\_database.rs](/worker/src/transactions/ddl/create_database.rs) | Rust | 100 | 43 | 18 | 161 |
| [worker/src/transactions/ddl/create\_schema.rs](/worker/src/transactions/ddl/create_schema.rs) | Rust | 107 | 34 | 17 | 158 |
| [worker/src/transactions/ddl/create\_table.rs](/worker/src/transactions/ddl/create_table.rs) | Rust | 256 | 62 | 33 | 351 |
| [worker/src/transactions/ddl/mod.rs](/worker/src/transactions/ddl/mod.rs) | Rust | 3 | 0 | 1 | 4 |
| [worker/src/transactions/maestro.rs](/worker/src/transactions/maestro.rs) | Rust | 1,032 | 39 | 72 | 1,143 |
| [worker/src/transactions/mod.rs](/worker/src/transactions/mod.rs) | Rust | 3 | 0 | 1 | 4 |
| [worker/src/txn/manifest.rs](/worker/src/txn/manifest.rs) | Rust | 12 | 5 | 5 | 22 |
| [worker/src/txn/mod.rs](/worker/src/txn/mod.rs) | Rust | 1 | 0 | 1 | 2 |
| [worker/tests/execution\_type\_coercion\_tests.rs](/worker/tests/execution_type_coercion_tests.rs) | Rust | 261 | 49 | 29 | 339 |
| [worker/tests/staging\_tests.rs](/worker/tests/staging_tests.rs) | Rust | 23 | 2 | 5 | 30 |

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)