import serverFixture from '../fixtures/server.json';
import sessionsFixture from '../fixtures/sessions.json';
import workersFixture from '../fixtures/workers.json';
import { DashboardKey } from '../types/dashboard';

const API_BASE = '/dashboard';
const DASHBOARD_SOURCE_ENV = 'VITE_KIONAS_DASHBOARD_SOURCE';

type DashboardSource = 'live' | 'fixture';

function resolveDashboardSource(): DashboardSource {
	const source = import.meta.env[DASHBOARD_SOURCE_ENV] as string | undefined;
	return source === 'fixture' ? 'fixture' : 'live';
}

const FIXTURE_BY_KEY: Partial<Record<DashboardKey, unknown>> = {
	server_stats: serverFixture,
	sessions: sessionsFixture,
	workers: workersFixture
};

export async function fetchDashboardKey(key: DashboardKey): Promise<unknown> {
	const source = resolveDashboardSource();

	if (source === 'fixture' && FIXTURE_BY_KEY[key] !== undefined) {
		return FIXTURE_BY_KEY[key];
	}

	const response = await fetch(`${API_BASE}/key?name=${encodeURIComponent(key)}`);
	if (!response.ok) {
		throw new Error(`Failed to fetch ${key}: ${response.statusText}`);
	}

	return response.json();
}

export function isFixtureSourceEnabled(): boolean {
	return resolveDashboardSource() === 'fixture';
}
