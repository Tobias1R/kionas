import { useQuery } from '@tanstack/react-query';

import { fetchDashboardKey } from '../services/dashboardSource';
import { DashboardKey } from '../types/dashboard';

/**
 * Hook to fetch dashboard data from backend
 * @param key - The dashboard key to fetch (e.g., 'server_stats', 'sessions')
 */
export function useDashboardData<TData = unknown>(key: DashboardKey) {
	return useQuery({
		queryKey: ['dashboard', key],
		queryFn: () => fetchDashboardKey(key) as Promise<TData>,
		refetchInterval: 15000, // Refresh every 15 seconds
		retry: 1
	});
}
