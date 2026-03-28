import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Grid from '@mui/material/Grid';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Chip from '@mui/material/Chip';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { lighten, useTheme } from '@mui/material/styles';

import { useSessionsData } from '../../../api/hooks/useSessionsData';
import { formatDateTime, formatDurationMs, formatNumber } from '../../../lib/formatters';

function MetricCard({ label, value }: { label: string; value: string | number }) {
	return (
		<Box
			sx={{
				p: 2,
				borderRadius: 2,
				bgcolor: 'background.default'
			}}
		>
			<Typography variant="caption" color="text.secondary">
				{label}
			</Typography>
			<Typography variant="h6" sx={{ fontWeight: 700 }}>
				{value}
			</Typography>
		</Box>
	);
}

/**
 * Sessions Widget - displays active session count and info
 */
function SessionsWidget() {
	const { sessions, meta, isLoading, error } = useSessionsData();
	const theme = useTheme();

	const activeCount = sessions.length;
	const totalQueries = sessions.reduce((sum, session) => sum + session.query_count, 0);
	const totalErrors = sessions.reduce((sum, session) => sum + session.error_count, 0);
	const totalDurationMs = sessions.reduce((sum, session) => sum + session.total_query_duration_ms, 0);
	const averageDurationMs = totalQueries > 0 ? Math.round(totalDurationMs / totalQueries) : 0;

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load sessions</Alert>
			</Paper>
		);
	}

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
			<div className="flex flex-1 flex-col items-center justify-center p-6">
				{isLoading ? (
					<CircularProgress />
				) : (
					<Box className="w-full space-y-6">
						<Grid container spacing={2}>
							<Grid size={{ xs: 12, sm: 6 }}>
								<MetricCard label="Active Sessions" value={formatNumber(activeCount)} />
							</Grid>
							<Grid size={{ xs: 12, sm: 6 }}>
								<MetricCard label="Total Queries" value={formatNumber(totalQueries)} />
							</Grid>
							<Grid size={{ xs: 12, sm: 6 }}>
								<MetricCard label="Avg Query Duration" value={formatDurationMs(averageDurationMs)} />
							</Grid>
							<Grid size={{ xs: 12, sm: 6 }}>
								<MetricCard label="Total Errors" value={formatNumber(totalErrors)} />
							</Grid>
						</Grid>

						<Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
							<Chip label={`Snapshot: ${formatDateTime(meta.generatedAt)}`} size="small" variant="outlined" />
							{meta.partialFailure && <Chip label="Partial data" color="warning" size="small" />}
						</Box>

						<TableContainer sx={{ maxHeight: 320, borderRadius: 2, border: '1px solid', borderColor: 'divider' }}>
							<Table size="small" stickyHeader>
								<TableHead>
									<TableRow>
										<TableCell>Session</TableCell>
										<TableCell>Role</TableCell>
										<TableCell>Warehouse</TableCell>
										<TableCell align="right">Queries</TableCell>
										<TableCell align="right">Errors</TableCell>
										<TableCell align="right">Avg Duration</TableCell>
										<TableCell>Last Query</TableCell>
									</TableRow>
								</TableHead>
								<TableBody>
									{sessions.map((session) => {
										const avgDuration =
											session.query_count > 0
												? Math.round(session.total_query_duration_ms / session.query_count)
												: 0;

										return (
											<TableRow key={session.id} hover>
												<TableCell>
													<Typography sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
														{session.id.slice(0, 8)}...
													</Typography>
												</TableCell>
												<TableCell>{session.role}</TableCell>
												<TableCell>{session.warehouse}</TableCell>
												<TableCell align="right">{formatNumber(session.query_count)}</TableCell>
												<TableCell align="right">{formatNumber(session.error_count)}</TableCell>
												<TableCell align="right">{formatDurationMs(avgDuration)}</TableCell>
												<TableCell>{formatDateTime(session.last_query_at)}</TableCell>
											</TableRow>
										);
									})}
									{sessions.length === 0 && (
										<TableRow>
											<TableCell colSpan={7} align="center">
												<Typography color="text.secondary">No active sessions</Typography>
											</TableCell>
										</TableRow>
									)}
								</TableBody>
							</Table>
						</TableContainer>
					</Box>
				)}
			</div>
		</Paper>
	);
}

export default SessionsWidget;
