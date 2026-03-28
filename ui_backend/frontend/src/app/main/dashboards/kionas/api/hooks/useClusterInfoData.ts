import { useMemo } from 'react';

import { extractEnvelopeMeta, normalizeClusterInfo } from '../models/normalizeDashboardPayload';
import { useDashboardData } from './useDashboardData';

export function useClusterInfoData() {
	const query = useDashboardData('cluster_info');

	const clusterInfo = useMemo(() => normalizeClusterInfo(query.data), [query.data]);
	const meta = useMemo(() => extractEnvelopeMeta(query.data), [query.data]);

	return {
		...query,
		clusterInfo,
		meta
	};
}
