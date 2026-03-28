import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import LinearProgress from '@mui/material/LinearProgress';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { lighten, useTheme } from '@mui/material/styles';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

interface SessionData {
	count: number;
	[key: string]: any;
}

/**
 * Sessions Widget - displays active session count and info
 */
function SessionsWidget() {
	const { data, isLoading, error } = useDashboardData('sessions');
	const theme = useTheme();
	const sessionData = (data as SessionData) || { count: 0 };
	const count = sessionData?.count || 0;

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load sessions</Alert>
			</Paper>
		);
	}

	// Estimate session utilization (mock for now - you can update this based on actual max capacity)
	const maxSessions = 100;
	const utilizationPercent = Math.round((count / maxSessions) * 100);
	const statusColor = utilizationPercent > 80 ? 'error' : utilizationPercent > 50 ? 'warning' : 'success';

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Header */}
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">
					Active Sessions
				</Typography>
				<FuseSvgIcon className="text-purple-500">lucide:users</FuseSvgIcon>
			</div>

			{/* Content */}
			<div className="flex flex-1 flex-col items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : (
					<Box className="w-full space-y-6">
						{/* Big Number Display */}
						<Box sx={{ textAlign: 'center' }}>
							<Typography
								sx={{
									fontSize: { xs: '2.5rem', sm: '3.5rem', md: '4rem' },
									fontWeight: 700,
									letterSpacing: '-0.05em',
									background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`,
									WebkitBackgroundClip: 'text',
									WebkitTextFillColor: 'transparent'
								}}
							>
								{count}
							</Typography>
							<Typography
								variant="subtitle2"
								sx={{
									marginTop: '8px',
									color: 'textSecondary',
									fontSize: '0.95rem'
								}}
							>
								sessions active
							</Typography>
						</Box>

						{/* Utilization Bar */}
						<Box
							sx={{
								backgroundColor: lighten(theme.palette.background.default, 0.5),
								borderRadius: '12px',
								padding: '16px',
								space: '8px'
							}}
						>
							<Box sx={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
								<Typography variant="body2" sx={{ fontWeight: 500 }}>
									Capacity Utilization
								</Typography>
								<Typography variant="body2" sx={{ fontWeight: 600, color: statusColor }}>
									{utilizationPercent}%
								</Typography>
							</Box>
							<LinearProgress
								variant="determinate"
								value={utilizationPercent}
								sx={{
									height: '8px',
									borderRadius: '4px',
									backgroundColor: lighten(theme.palette.primary.main, 0.7),
									'& .MuiLinearProgress-bar': {
										borderRadius: '4px'
									}
								}}
								color={statusColor === 'success' ? 'success' : statusColor === 'warning' ? 'warning' : 'error'}
							/>
							<Typography
								variant="caption"
								sx={{
									display: 'block',
									marginTop: '8px',
									color: 'textSecondary'
								}}
							>
								{count} of {maxSessions} sessions
							</Typography>
						</Box>

						{/* Status Badge */}
						<Box
							sx={{
								display: 'flex',
								gap: '12px',
								justifyContent: 'center',
								flexWrap: 'wrap',
								backgroundColor: lighten(theme.palette.background.default, 0.5),
								borderRadius: '12px',
								padding: '12px'
							}}
						>
							<Box sx={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
								<Box
									sx={{
										width: '12px',
										height: '12px',
										borderRadius: '50%',
										backgroundColor:
											statusColor === 'success'
												? theme.palette.success.main
												: statusColor === 'warning'
													? theme.palette.warning.main
													: theme.palette.error.main
									}}
								/>
								<Typography variant="caption" sx={{ fontWeight: 500 }}>
									{statusColor === 'success'
										? 'Optimal'
										: statusColor === 'warning'
											? 'Elevated'
											: 'Critical'}
								</Typography>
							</Box>
						</Box>
					</Box>
				)}
			</div>
		</Paper>
	);
}

export default SessionsWidget;
