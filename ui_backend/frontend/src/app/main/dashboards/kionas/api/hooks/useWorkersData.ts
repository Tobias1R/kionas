import { useMemo } from 'react';

import { normalizeWorkers } from '../models/normalizeDashboardPayload';
import { useDashboardData } from './useDashboardData';

export function useWorkersData() {
	const query = useDashboardData('workers');

	const workers = useMemo(() => normalizeWorkers(query.data), [query.data]);

	return {
		...query,
		workers
	};
}
