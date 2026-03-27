# Details

Date : 2026-03-27 14:34:43

Directory c:\\code\\kionas\\server

Total : 66 files,  14221 codes, 2181 comments, 1566 blanks, all 17968 lines

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [server/Cargo.toml](/server/Cargo.toml) | TOML | 36 | 1 | 4 | 41 |
| [server/build.rs](/server/build.rs) | Rust | 19 | 0 | 2 | 21 |
| [server/src/auth/jwt.rs](/server/src/auth/jwt.rs) | Rust | 52 | 1 | 9 | 62 |
| [server/src/auth/mod.rs](/server/src/auth/mod.rs) | Rust | 22 | 0 | 6 | 28 |
| [server/src/auth/persistence.rs](/server/src/auth/persistence.rs) | Rust | 123 | 0 | 22 | 145 |
| [server/src/auth\_setup.rs](/server/src/auth_setup.rs) | Rust | 24 | 2 | 5 | 31 |
| [server/src/consul.rs](/server/src/consul.rs) | Rust | 52 | 0 | 7 | 59 |
| [server/src/handlers.rs](/server/src/handlers.rs) | Rust | 66 | 1 | 8 | 75 |
| [server/src/janitor/mod.rs](/server/src/janitor/mod.rs) | Rust | 411 | 202 | 55 | 668 |
| [server/src/lib.rs](/server/src/lib.rs) | Rust | 18 | 1 | 1 | 20 |
| [server/src/main.rs](/server/src/main.rs) | Rust | 40 | 7 | 8 | 55 |
| [server/src/parser/mod.rs](/server/src/parser/mod.rs) | Rust | 5 | 0 | 2 | 7 |
| [server/src/parser/sql.rs](/server/src/parser/sql.rs) | Rust | 44 | 0 | 4 | 48 |
| [server/src/planner/engine.rs](/server/src/planner/engine.rs) | Rust | 1,198 | 176 | 159 | 1,533 |
| [server/src/planner/mod.rs](/server/src/planner/mod.rs) | Rust | 9 | 0 | 3 | 12 |
| [server/src/planner/stage\_extractor.rs](/server/src/planner/stage_extractor.rs) | Rust | 109 | 66 | 15 | 190 |
| [server/src/providers/context\_builder.rs](/server/src/providers/context_builder.rs) | Rust | 71 | 9 | 13 | 93 |
| [server/src/providers/identifier.rs](/server/src/providers/identifier.rs) | Rust | 8 | 10 | 1 | 19 |
| [server/src/providers/metastore\_resolver.rs](/server/src/providers/metastore_resolver.rs) | Rust | 98 | 24 | 15 | 137 |
| [server/src/providers/mod.rs](/server/src/providers/mod.rs) | Rust | 13 | 0 | 3 | 16 |
| [server/src/providers/relation\_metadata.rs](/server/src/providers/relation_metadata.rs) | Rust | 57 | 31 | 9 | 97 |
| [server/src/providers/table\_provider.rs](/server/src/providers/table_provider.rs) | Rust | 88 | 13 | 13 | 114 |
| [server/src/providers/type\_mapping.rs](/server/src/providers/type_mapping.rs) | Rust | 32 | 10 | 4 | 46 |
| [server/src/server.rs](/server/src/server.rs) | Rust | 155 | 13 | 26 | 194 |
| [server/src/services/interops\_service\_server.rs](/server/src/services/interops_service_server.rs) | Rust | 239 | 17 | 21 | 277 |
| [server/src/services/metastore\_client.rs](/server/src/services/metastore_client.rs) | Rust | 65 | 5 | 13 | 83 |
| [server/src/services/mod.rs](/server/src/services/mod.rs) | Rust | 6 | 0 | 1 | 7 |
| [server/src/services/request\_context.rs](/server/src/services/request_context.rs) | Rust | 64 | 2 | 9 | 75 |
| [server/src/services/warehouse\_auth\_service.rs](/server/src/services/warehouse_auth_service.rs) | Rust | 257 | 21 | 18 | 296 |
| [server/src/services/warehouse\_service\_server.rs](/server/src/services/warehouse_service_server.rs) | Rust | 159 | 35 | 26 | 220 |
| [server/src/services/worker\_service\_client.rs](/server/src/services/worker_service_client.rs) | Rust | 4 | 1 | 1 | 6 |
| [server/src/session.rs](/server/src/session.rs) | Rust | 239 | 35 | 38 | 312 |
| [server/src/statement\_handler/ddl/create\_database.rs](/server/src/statement_handler/ddl/create_database.rs) | Rust | 276 | 91 | 22 | 389 |
| [server/src/statement\_handler/ddl/create\_schema.rs](/server/src/statement_handler/ddl/create_schema.rs) | Rust | 328 | 78 | 25 | 431 |
| [server/src/statement\_handler/ddl/create\_table.rs](/server/src/statement_handler/ddl/create_table.rs) | Rust | 814 | 77 | 29 | 920 |
| [server/src/statement\_handler/ddl/mod.rs](/server/src/statement_handler/ddl/mod.rs) | Rust | 3 | 0 | 1 | 4 |
| [server/src/statement\_handler/dml/insert.rs](/server/src/statement_handler/dml/insert.rs) | Rust | 327 | 58 | 30 | 415 |
| [server/src/statement\_handler/dml/mod.rs](/server/src/statement_handler/dml/mod.rs) | Rust | 1 | 0 | 1 | 2 |
| [server/src/statement\_handler/mod.rs](/server/src/statement_handler/mod.rs) | Rust | 206 | 11 | 10 | 227 |
| [server/src/statement\_handler/query/mod.rs](/server/src/statement_handler/query/mod.rs) | Rust | 1 | 0 | 1 | 2 |
| [server/src/statement\_handler/query/select.rs](/server/src/statement_handler/query/select.rs) | Rust | 1,559 | 579 | 166 | 2,304 |
| [server/src/statement\_handler/shared/distributed\_dag.rs](/server/src/statement_handler/shared/distributed_dag.rs) | Rust | 375 | 147 | 48 | 570 |
| [server/src/statement\_handler/shared/helpers.rs](/server/src/statement_handler/shared/helpers.rs) | Rust | 874 | 90 | 91 | 1,055 |
| [server/src/statement\_handler/shared/mod.rs](/server/src/statement_handler/shared/mod.rs) | Rust | 2 | 0 | 1 | 3 |
| [server/src/statement\_handler/utility/mod.rs](/server/src/statement_handler/utility/mod.rs) | Rust | 2 | 0 | 1 | 3 |
| [server/src/statement\_handler/utility/rbac.rs](/server/src/statement_handler/utility/rbac.rs) | Rust | 261 | 79 | 17 | 357 |
| [server/src/statement\_handler/utility/use\_warehouse.rs](/server/src/statement_handler/utility/use_warehouse.rs) | Rust | 55 | 2 | 6 | 63 |
| [server/src/tasks/mod.rs](/server/src/tasks/mod.rs) | Rust | 542 | 58 | 49 | 649 |
| [server/src/tests/planner\_engine\_tests.rs](/server/src/tests/planner_engine_tests.rs) | Rust | 707 | 0 | 92 | 799 |
| [server/src/tests/planner\_stage\_extractor\_tests.rs](/server/src/tests/planner_stage_extractor_tests.rs) | Rust | 119 | 10 | 21 | 150 |
| [server/src/tests/providers\_module\_tests.rs](/server/src/tests/providers_module_tests.rs) | Rust | 68 | 0 | 7 | 75 |
| [server/src/tests/services\_warehouse\_service\_server\_tests.rs](/server/src/tests/services_warehouse_service_server_tests.rs) | Rust | 198 | 0 | 26 | 224 |
| [server/src/tests/spike\_union\_exec.rs](/server/src/tests/spike_union_exec.rs) | Rust | 141 | 15 | 41 | 197 |
| [server/src/tests/statement\_handler\_dml\_insert\_tests.rs](/server/src/tests/statement_handler_dml_insert_tests.rs) | Rust | 43 | 0 | 5 | 48 |
| [server/src/tests/statement\_handler\_query\_select\_tests.rs](/server/src/tests/statement_handler_query_select_tests.rs) | Rust | 530 | 0 | 42 | 572 |
| [server/src/tests/statement\_handler\_shared\_distributed\_dag\_tests.rs](/server/src/tests/statement_handler_shared_distributed_dag_tests.rs) | Rust | 571 | 0 | 36 | 607 |
| [server/src/tests/statement\_handler\_shared\_helpers\_tests.rs](/server/src/tests/statement_handler_shared_helpers_tests.rs) | Rust | 959 | 2 | 123 | 1,084 |
| [server/src/tests/tasks\_mod\_tests.rs](/server/src/tests/tasks_mod_tests.rs) | Rust | 302 | 0 | 29 | 331 |
| [server/src/tls.rs](/server/src/tls.rs) | Rust | 70 | 1 | 15 | 86 |
| [server/src/transactions/maestro.rs](/server/src/transactions/maestro.rs) | Rust | 405 | 68 | 40 | 513 |
| [server/src/transactions/mod.rs](/server/src/transactions/mod.rs) | Rust | 1 | 2 | 2 | 5 |
| [server/src/warehouse/mod.rs](/server/src/warehouse/mod.rs) | Rust | 97 | 3 | 17 | 117 |
| [server/src/warehouse/pool.rs](/server/src/warehouse/pool.rs) | Rust | 60 | 63 | 7 | 130 |
| [server/src/warehouse/state.rs](/server/src/warehouse/state.rs) | Rust | 307 | 62 | 28 | 397 |
| [server/src/workers/mod.rs](/server/src/workers/mod.rs) | Rust | 171 | 1 | 8 | 180 |
| [server/src/workers\_pool.rs](/server/src/workers_pool.rs) | Rust | 63 | 1 | 8 | 72 |

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)