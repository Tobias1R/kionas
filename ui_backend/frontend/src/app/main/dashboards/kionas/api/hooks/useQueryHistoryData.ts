import { useMemo } from 'react';

import { normalizeQueryHistory } from '../models/normalizeDashboardPayload';
import { useDashboardData } from './useDashboardData';

export function useQueryHistoryData() {
	const query = useDashboardData('query_history');

	const rows = useMemo(() => normalizeQueryHistory(query.data), [query.data]);

	return {
		...query,
		rows
	};
}
