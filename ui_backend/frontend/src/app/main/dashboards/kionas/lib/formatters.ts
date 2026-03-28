export function formatNumber(value: number): string {
	return new Intl.NumberFormat('en-US').format(value);
}

export function formatPercent(value: number, fractionDigits = 1): string {
	return `${value.toFixed(fractionDigits)}%`;
}

export function formatDurationMs(value: number): string {
	if (value < 1000) {
		return `${value} ms`;
	}

	const seconds = Math.floor(value / 1000);
	const remainingMs = value % 1000;
	if (seconds < 60) {
		return `${seconds}s ${remainingMs}ms`;
	}

	const minutes = Math.floor(seconds / 60);
	const remainingSeconds = seconds % 60;
	return `${minutes}m ${remainingSeconds}s`;
}

export function formatBytes(bytes: number): string {
	if (bytes <= 0) {
		return '0 B';
	}

	const units = ['B', 'KB', 'MB', 'GB', 'TB'];
	const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
	const value = bytes / 1024 ** index;
	return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

export function formatDateTime(value?: string | number): string {
	if (!value) {
		return '-';
	}

	const date = new Date(value);
	if (Number.isNaN(date.getTime())) {
		return '-';
	}

	return date.toLocaleString();
}

export function formatUptime(seconds: number): string {
	if (seconds <= 0) {
		return '0m';
	}

	const days = Math.floor(seconds / 86400);
	const hours = Math.floor((seconds % 86400) / 3600);
	const minutes = Math.floor((seconds % 3600) / 60);

	if (days > 0) {
		return `${days}d ${hours}h`;
	}
	if (hours > 0) {
		return `${hours}h ${minutes}m`;
	}
	return `${minutes}m`;
}
