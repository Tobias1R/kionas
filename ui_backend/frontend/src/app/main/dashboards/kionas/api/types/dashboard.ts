export type DashboardKey =
	| 'server_stats'
	| 'sessions'
	| 'workers'
	| 'tokens'
	| 'cluster_info'
	| 'consul_cluster_summary'
	| 'query_history';

export interface DashboardEnvelope<T> {
	generated_at: string;
	freshness_target_seconds: number;
	source_component: string;
	partial_failure: boolean;
	data_version: number;
	error: string | null;
	data: T | null;
}

export interface ServerStatsPayload {
	counter: number;
	warehouses: string[];
	worker_pools: string[];
	sessions: number;
	memory_mb: number;
	virtual_memory_mb: number;
	cpu_usage_percent: number;
	active_queries: number;
	total_queries_submitted: number;
	total_queries_succeeded: number;
	total_queries_failed: number;
	queries_per_minute: number;
}

export interface SessionSnapshot {
	id: string;
	role: string;
	warehouse: string;
	remote_addr: string;
	is_authenticated: boolean;
	last_active: number;
	use_database: string;
	query_count: number;
	last_query_at: number;
	total_query_duration_ms: number;
	error_count: number;
}

export type WorkerHealthStatus = 'Healthy' | 'Degraded' | 'Unhealthy';

export interface WorkerSystemInfo {
	worker_id: string;
	hostname: string;
	cluster_id: string;
	warehouse_pool?: string;
	memory_used_mb: number;
	memory_total_mb: number;
	cpu_percent: number;
	thread_count: number;
	disk_used_mb: number;
	disk_total_mb: number;
	health_status: WorkerHealthStatus;
	active_stages: number;
	total_stages_executed: number;
	active_partitions: number;
	total_partitions_executed: number;
	bytes_scanned_total: number;
	total_stage_exec_ms: number;
	total_rows_produced: number;
	started_at: string;
	uptime_seconds: number;
	updated_at: string;
}

export type ClusterHealthStatus = 'Healthy' | 'Degraded' | 'Unhealthy';

export interface ClusterInfoPayload {
	cluster_name: string;
	cluster_id: string;
	cluster_version: string;
	status: ClusterHealthStatus;
	uptime_seconds: number;
	started_at: string;
	node_count: number;
	updated_at: string;
}

export type QueryStatus = 'Running' | 'Succeeded' | 'Failed';

export interface QuerySummary {
	query_id: string;
	session_id: string;
	user_id: string;
	warehouse_id?: string;
	sql_digest: string;
	status: QueryStatus;
	duration_ms?: number;
	error?: string;
	timestamp: string;
}
