import { useQuery } from '@tanstack/react-query';

const API_BASE = '/dashboard';

export interface DashboardData {
	[key: string]: unknown;
}

/**
 * Hook to fetch dashboard data from backend
 * @param key - The dashboard key to fetch (e.g., 'server_stats', 'sessions')
 */
export function useDashboardData(key: string) {
	return useQuery({
		queryKey: ['dashboard', key],
		queryFn: async () => {
			const response = await fetch(`${API_BASE}/key?name=${encodeURIComponent(key)}`);
			if (!response.ok) {
				throw new Error(`Failed to fetch ${key}: ${response.statusText}`);
			}
			return response.json() as Promise<DashboardData>;
		},
		refetchInterval: 15000, // Refresh every 15 seconds
		retry: 1
	});
}
