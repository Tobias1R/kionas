import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import Grid from '@mui/material/Grid';
import Chip from '@mui/material/Chip';
import { lighten, useTheme } from '@mui/material/styles';

import { useServerStatsData } from '../../../api/hooks/useServerStatsData';
import { formatDateTime, formatNumber, formatPercent } from '../../../lib/formatters';

/**
 * Metric Card Component - displays a metric with icon and value
 */
function MetricCard({ icon, label, value, color = 'primary' }: {
	icon: string;
	label: string;
	value: string | number;
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
					{value}
				</Typography>
			</Box>
		</Box>
	);
}

/**
 * Server Stats Widget - displays current server statistics
 */
function ServerStatsWidget() {
	const { stats, meta, isLoading, error } = useServerStatsData();
	const theme = useTheme();

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load server stats</Alert>
			</Paper>
		);
	}

	const memoryPercent =
		stats.virtual_memory_mb > 0
			? (stats.memory_mb / stats.virtual_memory_mb) * 100
			: 0;

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
				) : (
					<Box className="w-full space-y-6">
						<Grid container spacing={2}>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard icon="lucide:cpu" label="CPU Usage" value={formatPercent(stats.cpu_usage_percent)} color="info" />
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:memory-stick"
									label="Memory"
									value={`${formatNumber(stats.memory_mb)} / ${formatNumber(stats.virtual_memory_mb)} MB`}
									color={memoryPercent > 85 ? 'error' : 'warning'}
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:activity"
									label="Queries / Minute"
									value={stats.queries_per_minute.toFixed(1)}
									color="success"
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:workflow"
									label="Worker Pools"
									value={stats.worker_pools.length}
									color="primary"
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:users"
									label="Active Sessions"
									value={formatNumber(stats.sessions)}
									color="info"
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:send"
									label="Submitted Queries"
									value={formatNumber(stats.total_queries_submitted)}
									color="primary"
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:check-circle"
									label="Succeeded Queries"
									value={formatNumber(stats.total_queries_succeeded)}
									color="success"
								/>
							</Grid>
							<Grid size={{ xs: 12, sm: 6, lg: 3 }}>
								<MetricCard
									icon="lucide:alert-triangle"
									label="Failed Queries"
									value={formatNumber(stats.total_queries_failed)}
									color={stats.total_queries_failed > 0 ? 'error' : 'success'}
								/>
							</Grid>
						</Grid>

						<Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
							<Chip label={`Active queries: ${stats.active_queries}`} color="primary" variant="outlined" />
							<Chip label={`Warehouses: ${stats.warehouses.length}`} color="info" variant="outlined" />
							{meta.partialFailure && <Chip label="Partial data" color="warning" variant="filled" />}
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
								Last updated: {formatDateTime(meta.generatedAt)}
							</Typography>
						</Box>
					</Box>
				)}
			</div>
		</Paper>
	);
}

export default ServerStatsWidget;
