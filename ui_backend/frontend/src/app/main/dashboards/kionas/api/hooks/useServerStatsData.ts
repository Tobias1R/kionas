import { useMemo } from 'react';

import { extractEnvelopeMeta, normalizeServerStats } from '../models/normalizeDashboardPayload';
import { useDashboardData } from './useDashboardData';

export function useServerStatsData() {
	const query = useDashboardData('server_stats');

	const stats = useMemo(() => normalizeServerStats(query.data), [query.data]);
	const meta = useMemo(() => extractEnvelopeMeta(query.data), [query.data]);

	return {
		...query,
		stats,
		meta
	};
}
