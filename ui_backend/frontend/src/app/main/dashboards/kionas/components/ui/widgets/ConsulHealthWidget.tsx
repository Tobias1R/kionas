import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Chip from '@mui/material/Chip';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { lighten, useTheme } from '@mui/material/styles';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

interface HealthData {
	status: 'passing' | 'warning' | 'critical';
	[key: string]: any;
}

/**
 * Get color for health status
 */
function getStatusColor(status: string): 'success' | 'warning' | 'error' {
	switch (status) {
		case 'passing':
			return 'success';
		case 'warning':
			return 'warning';
		case 'critical':
			return 'error';
		default:
			return 'warning' as const;
	}
}

/**
 * Get icon for health status
 */
function getStatusIcon(status: string): string {
	switch (status) {
		case 'passing':
			return 'lucide:check-circle';
		case 'warning':
			return 'lucide:alert-triangle';
		case 'critical':
			return 'lucide:alert-circle';
		default:
			return 'lucide:help-circle';
	}
}

/**
 * Health Badge Component
 */
function HealthBadge({ status }: { status: string }) {
	const theme = useTheme();
	const color = getStatusColor(status);
	const icon = getStatusIcon(status);
	const colorMap = {
		success: theme.palette.success.main,
		warning: theme.palette.warning.main,
		error: theme.palette.error.main
	};

	return (
		<Box
			sx={{
				display: 'inline-flex',
				alignItems: 'center',
				gap: '12px',
				backgroundColor: lighten(colorMap[color], 0.85),
				border: `2px solid ${colorMap[color]}`,
				borderRadius: '16px',
				padding: '12px 24px'
			}}
		>
			<FuseSvgIcon sx={{ color: colorMap[color], fontSize: '28px' }}>
				{icon}
			</FuseSvgIcon>
			<Typography
				sx={{
					fontSize: '1.25rem',
					fontWeight: 700,
					color: colorMap[color],
					textTransform: 'uppercase',
					letterSpacing: '0.05em'
				}}
			>
				{status}
			</Typography>
		</Box>
	);
}

/**
 * Consul Health Widget - displays cluster health status
 */
function ConsulHealthWidget() {
	const { data, isLoading, error } = useDashboardData('consul_cluster_summary');
	const theme = useTheme();
	const healthData = (data as HealthData) || { status: 'unknown' };
	const status = healthData?.status || 'unknown';

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load Consul health</Alert>
			</Paper>
		);
	}

	const statusColor = getStatusColor(status);
	const colorMap = {
		success: theme.palette.success.main,
		warning: theme.palette.warning.main,
		error: theme.palette.error.main
	};

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Header */}
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">
					Cluster Health
				</Typography>
				<FuseSvgIcon className="text-orange-500">lucide:heart-pulse</FuseSvgIcon>
			</div>

			{/* Content */}
			<div className="flex flex-1 flex-col items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : (
					<Box className="w-full space-y-6">
						{/* Health Badge */}
						<Box sx={{ display: 'flex', justifyContent: 'center' }}>
							<HealthBadge status={status} />
						</Box>

						{/* Status Details */}
						<Box
							sx={{
								backgroundColor: lighten(colorMap[statusColor], 0.9),
								borderRadius: '12px',
								padding: '16px',
								border: `1px solid ${lighten(colorMap[statusColor], 0.6)}`
							}}
						>
							<Typography variant="subtitle2" sx={{ marginBottom: '12px', fontWeight: 600 }}>
								Status Details
							</Typography>
							<Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: '12px' }}>
								<Box>
									<Typography variant="caption" color="textSecondary">
										Cluster State
									</Typography>
									<Typography variant="body2" sx={{ fontWeight: 500, marginTop: '4px' }}>
										{status === 'passing' ? '🟢 Healthy' : status === 'warning' ? '🟡 Degraded' : '🔴 Unhealthy'}
									</Typography>
								</Box>
								<Box>
									<Typography variant="caption" color="textSecondary">
										Last Updated
									</Typography>
									<Typography variant="body2" sx={{ fontWeight: 500, marginTop: '4px', fontSize: '0.85rem' }}>
										{new Date().toLocaleTimeString()}
									</Typography>
								</Box>
							</Box>
						</Box>

						{/* Status Indicators */}
						<Box sx={{ display: 'flex', gap: '12px', flexWrap: 'wrap', justifyContent: 'center' }}>
							<Chip
								icon={<FuseSvgIcon fontSize="small">lucide:server</FuseSvgIcon>}
								label="Cluster OK"
								color={statusColor}
								variant="outlined"
								sx={{ fontWeight: 500 }}
							/>
							{status === 'passing' && (
								<Chip
									icon={<FuseSvgIcon fontSize="small">lucide:shield-check</FuseSvgIcon>}
									label="All Services"
									color="success"
									variant="filled"
									sx={{ fontWeight: 500 }}
								/>
							)}
						</Box>
					</Box>
				)}
			</div>
		</Paper>
	);
}

export default ConsulHealthWidget;
