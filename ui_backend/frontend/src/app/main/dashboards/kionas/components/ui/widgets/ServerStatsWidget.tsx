import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { lighten, useTheme } from '@mui/material/styles';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

interface ServerData {
	counter: number;
	warehouses: string[];
	worker_pools: string[];
	sessions: number;
	memory_mb: number;
	virtual_memory_mb: number;
	cpu_usage_percent: number;
}

interface ServerStatsResponse {
	data: ServerData;
	generated_at: string;
	freshness_target_seconds: number;
	source_component: string;
	partial_failure: boolean;
	data_version: number;
	error: string | null;
}

/**
 * Metric Card Component - displays a metric with icon and value
 */
function MetricCard({ icon, label, value, unit = '', color = 'primary' }: {
	icon: string;
	label: string;
	value: string | number;
	unit?: string;
	color?: 'primary' | 'success' | 'warning' | 'error' | 'info';
}) {
	const theme = useTheme();
	const colorMap = {
		primary: theme.palette.primary.main,
		success: theme.palette.success.main,
		warning: theme.palette.warning.main,
		error: theme.palette.error.main,
		info: theme.palette.info.main
	};

	return (
		<Box
			sx={{
				backgroundColor: lighten(colorMap[color], 0.9),
				borderRadius: '12px',
				padding: '16px',
				display: 'flex',
				alignItems: 'center',
				gap: '12px'
			}}
		>
			<Box
				sx={{
					display: 'flex',
					alignItems: 'center',
					justifyContent: 'center',
					width: '48px',
					height: '48px',
					backgroundColor: colorMap[color],
					borderRadius: '8px',
					color: 'white',
					flexShrink: 0
				}}
			>
				<FuseSvgIcon>{icon}</FuseSvgIcon>
			</Box>
			<Box sx={{ flex: 1, minWidth: 0 }}>
				<Typography variant="body2" color="textSecondary">
					{label}
				</Typography>
				<Typography variant="h6" sx={{ fontWeight: 600 }}>
					{value}{unit && <span style={{ fontSize: '0.85em', marginLeft: '4px' }}>{unit}</span>}
				</Typography>
			</Box>
		</Box>
	);
}

/**
 * Progress Metric Component - displays metric with circular progress
 */
function ProgressMetric({ icon, label, value, max = 100, color = 'primary' }: {
	icon: string;
	label: string;
	value: number;
	max: number;
	color?: 'primary' | 'success' | 'warning' | 'error' | 'info';
}) {
	const theme = useTheme();
	const percentage = Math.round((value / max) * 100);
	const colorMap = {
		primary: theme.palette.primary.main,
		success: theme.palette.success.main,
		warning: theme.palette.warning.main,
		error: theme.palette.error.main,
		info: theme.palette.info.main
	};

	let statusColor = 'success';
	if (percentage > 80) statusColor = 'error';
	else if (percentage > 60) statusColor = 'warning';

	return (
		<Box sx={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
			<Box sx={{ position: 'relative', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
				<CircularProgress
					variant="determinate"
					value={100}
					sx={{ color: lighten(colorMap[color], 0.7), position: 'absolute' }}
					size={80}
					thickness={3}
				/>
				<CircularProgress
					variant="determinate"
					value={percentage}
					sx={{ color: colorMap[color] }}
					size={80}
					thickness={3}
				/>
				<Box
					sx={{
						position: 'absolute',
						display: 'flex',
						flexDirection: 'column',
						alignItems: 'center'
					}}
				>
					<Typography variant="h6" sx={{ fontWeight: 600 }}>
						{percentage}%
					</Typography>
					<Typography variant="caption" color="textSecondary">
						{Math.round(value)}MB
					</Typography>
				</Box>
			</Box>
			<Box>
				<Typography variant="body2" color="textSecondary">
					{label}
				</Typography>
				<Typography variant="body2" sx={{ marginTop: '4px' }}>
					{Math.round(value)} / {max} MB
				</Typography>
			</Box>
		</Box>
	);
}

/**
 * Server Stats Widget - displays current server statistics
 */
function ServerStatsWidget() {
	const { data, isLoading, error } = useDashboardData('server_stats');
	const theme = useTheme();

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load server stats</Alert>
			</Paper>
		);
	}

	const serverData = (data as unknown) as ServerStatsResponse | undefined;
	const stats = serverData?.data as ServerData | undefined;

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Header */}
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">
					Server Stats
				</Typography>
				<FuseSvgIcon className="text-blue-500">lucide:server</FuseSvgIcon>
			</div>

			{/* Content */}
			<div className="flex flex-1 flex-col items-center justify-center p-6">
				{isLoading ? (
					<CircularProgress />
				) : stats ? (
					<Box className="w-full space-y-6">
						{/* CPU and Memory Status */}
						<Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr', md: '1fr 1fr 1fr' }, gap: '16px' }}>
							<Box
								sx={{
									display: 'flex',
									justifyContent: 'center',
									padding: '16px',
									backgroundColor: lighten(theme.palette.primary.main, 0.95),
									borderRadius: '12px'
								}}
							>
								<ProgressMetric
									icon="lucide:cpu"
									label="CPU Usage"
									value={stats.cpu_usage_percent}
									max={100}
									color="info"
								/>
							</Box>

							<Box
								sx={{
									display: 'flex',
									justifyContent: 'center',
									padding: '16px',
									backgroundColor: lighten(theme.palette.warning.main, 0.95),
									borderRadius: '12px'
								}}
							>
								<ProgressMetric
									icon="lucide:memory-stick"
									label="Memory Usage"
									value={stats.memory_mb}
									max={stats.virtual_memory_mb}
									color="warning"
								/>
							</Box>

							<MetricCard
								icon="lucide:workflow"
								label="Worker Pools"
								value={stats.worker_pools.length}
								color="success"
							/>
						</Box>

						{/* Additional Stats */}
						<Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: '12px' }}>
							<MetricCard
								icon="lucide:users"
								label="Active Sessions"
								value={stats.sessions}
								color="info"
							/>
						</Box>

						{/* Timestamp */}
						<Box
							sx={{
								padding: '12px',
								backgroundColor: lighten(theme.palette.background.default, 0.5),
								borderRadius: '8px',
								textAlign: 'center'
							}}
						>
							<Typography variant="caption" color="textSecondary">
								Last updated:{' '}
								{new Date(serverData?.generated_at || '').toLocaleTimeString()}
							</Typography>
						</Box>
					</Box>
				) : (
					<Typography color="textSecondary">No data available</Typography>
				)}
			</div>
		</Paper>
	);
}

export default ServerStatsWidget;
