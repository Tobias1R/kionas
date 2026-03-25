use super::{parse_structured_query_handle, print_stage_latency_breakdown};

#[test]
fn parse_structured_query_handle_extracts_stage_latency_breakdown() {
    let handle = "flight://worker1:50052/query/sales/public/users/worker-1?session_id=s1&task_id=t1&latency_stage_0_queue_ms=12&latency_stage_0_exec_ms=45&latency_stage_0_network_ms=6&latency_stage_1_queue_ms=3&latency_stage_1_exec_ms=18&latency_stage_1_network_ms=2";

    let parsed = parse_structured_query_handle(handle)
        .expect("structured query handle with stage latency should parse");

    assert_eq!(parsed.stage_latency.len(), 2);

    let stage0 = &parsed.stage_latency[0];
    assert_eq!(stage0.stage_id, 0);
    assert_eq!(stage0.queue_ms, 12);
    assert_eq!(stage0.exec_ms, 45);
    assert_eq!(stage0.network_ms, 6);

    let stage1 = &parsed.stage_latency[1];
    assert_eq!(stage1.stage_id, 1);
    assert_eq!(stage1.queue_ms, 3);
    assert_eq!(stage1.exec_ms, 18);
    assert_eq!(stage1.network_ms, 2);
}

#[test]
fn parse_structured_query_handle_accepts_handle_without_stage_latency() {
    let handle =
        "flight://worker1:50052/query/sales/public/users/worker-1?session_id=s1&task_id=t1";

    let parsed = parse_structured_query_handle(handle)
        .expect("structured query handle without stage latency should parse");

    assert!(parsed.stage_latency.is_empty());
}

#[test]
fn print_stage_latency_breakdown_accepts_valid_handle() {
    let handle = "flight://worker1:50052/query/sales/public/users/worker-1?session_id=s1&task_id=t1&latency_stage_0_queue_ms=7&latency_stage_0_exec_ms=11&latency_stage_0_network_ms=5";

    let result = print_stage_latency_breakdown(handle);
    assert!(result.is_ok());
}
