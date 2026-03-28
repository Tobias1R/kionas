import { useMemo } from 'react';

import { extractEnvelopeMeta, normalizeSessions } from '../models/normalizeDashboardPayload';
import { useDashboardData } from './useDashboardData';

export function useSessionsData() {
	const query = useDashboardData('sessions');

	const sessions = useMemo(() => normalizeSessions(query.data), [query.data]);
	const meta = useMemo(() => extractEnvelopeMeta(query.data), [query.data]);

	return {
		...query,
		sessions,
		meta
	};
}
