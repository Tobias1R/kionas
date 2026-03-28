import {
	ClusterInfoPayload,
	DashboardEnvelope,
	ServerStatsPayload,
	SessionSnapshot,
	WorkerSystemInfo
} from '../types/dashboard';
import { QueryHistoryRow } from '../types/queryHistory';

export interface EnvelopeMeta {
	generatedAt?: string;
	partialFailure?: boolean;
	error?: string | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null;
}

export function isDashboardEnvelope<T>(payload: unknown): payload is DashboardEnvelope<T> {
	if (!isRecord(payload)) {
		return false;
	}

	return (
		typeof payload.generated_at === 'string' &&
		typeof payload.freshness_target_seconds === 'number' &&
		typeof payload.source_component === 'string' &&
		typeof payload.partial_failure === 'boolean' &&
		typeof payload.data_version === 'number' &&
		Object.prototype.hasOwnProperty.call(payload, 'data')
	);
}

export function extractEnvelopeData<T>(payload: unknown, fallback: T): T {
	if (!isDashboardEnvelope<T>(payload)) {
		return fallback;
	}

	return payload.data ?? fallback;
}

export function extractEnvelopeMeta(payload: unknown): EnvelopeMeta {
	if (!isDashboardEnvelope<unknown>(payload)) {
		return {};
	}

	return {
		generatedAt: payload.generated_at,
		partialFailure: payload.partial_failure,
		error: payload.error
	};
}

const DEFAULT_SERVER_STATS: ServerStatsPayload = {
	counter: 0,
	warehouses: [],
	worker_pools: [],
	sessions: 0,
	memory_mb: 0,
	virtual_memory_mb: 0,
	cpu_usage_percent: 0,
	active_queries: 0,
	total_queries_submitted: 0,
	total_queries_succeeded: 0,
	total_queries_failed: 0,
	queries_per_minute: 0
};

export function normalizeServerStats(payload: unknown): ServerStatsPayload {
	return extractEnvelopeData<ServerStatsPayload>(payload, DEFAULT_SERVER_STATS);
}

export function normalizeSessions(payload: unknown): SessionSnapshot[] {
	return extractEnvelopeData<SessionSnapshot[]>(payload, []);
}

export function normalizeWorkers(payload: unknown): WorkerSystemInfo[] {
	if (Array.isArray(payload)) {
		return payload as WorkerSystemInfo[];
	}

	return extractEnvelopeData<WorkerSystemInfo[]>(payload, []);
}

export function normalizeClusterInfo(payload: unknown): ClusterInfoPayload | undefined {
	if (isDashboardEnvelope<ClusterInfoPayload>(payload)) {
		return payload.data ?? undefined;
	}

	if (
		isRecord(payload) &&
		typeof payload.cluster_name === 'string' &&
		typeof payload.cluster_id === 'string' &&
		typeof payload.cluster_version === 'string' &&
		typeof payload.status === 'string' &&
		typeof payload.uptime_seconds === 'number' &&
		typeof payload.started_at === 'string' &&
		typeof payload.node_count === 'number' &&
		typeof payload.updated_at === 'string'
	) {
		return payload as unknown as ClusterInfoPayload;
	}

	return undefined;
}

export function normalizeQueryHistory(payload: unknown): QueryHistoryRow[] {
	if (!Array.isArray(payload)) {
		return [];
	}

	return payload as QueryHistoryRow[];
}
