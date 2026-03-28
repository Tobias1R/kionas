export type QueryStatus = 'Running' | 'Succeeded' | 'Failed';

export interface QueryHistoryRow {
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
