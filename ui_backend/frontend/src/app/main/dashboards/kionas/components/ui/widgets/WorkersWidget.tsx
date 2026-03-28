import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Chip from '@mui/material/Chip';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useTheme } from '@mui/material/styles';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

interface WorkerInfo {
	worker_id: string;
	hostname: string;
	cluster_id: string;
	warehouse_pool?: string;
	memory_used_mb: number;
	memory_total_mb: number;
	cpu_percent: number;
	disk_used_mb: number;
	disk_total_mb: number;
	health_status: 'Healthy' | 'Degraded' | 'Unhealthy';
	active_queries: number;
	total_queries_processed: number;
	started_at: string;
	uptime_seconds: number;
	updated_at: string;
}

/**
 * Get color for worker health status
 */
function getHealthStatusColor(status: string): 'success' | 'warning' | 'error' {
	switch (status) {
		case 'Healthy':
			return 'success';
		case 'Degraded':
			return 'warning';
		case 'Unhealthy':
			return 'error';
		default:
			return 'warning' as const;
	}
}

/**
 * Format uptime to human readable format
 */
function formatUptime(seconds: number): string {
	const hours = Math.floor(seconds / 3600);
	const minutes = Math.floor((seconds % 3600) / 60);
	if (hours > 0) {
		return `${hours}h ${minutes}m`;
	}
	return `${minutes}m`;
}

/**
 * Format memory usage
 */
function formatMemory(used: number, total: number): string {
	const percent = Math.round((used / total) * 100);
	return `${used} / ${total} MB (${percent}%)`;
}

/**
 * Workers Widget - displays active workers in the cluster
 */
function WorkersWidget() {
	const { data, isLoading, error } = useDashboardData('workers');
	const theme = useTheme();

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load workers</Alert>
			</Paper>
		);
	}

	const workers = ((data as unknown) as WorkerInfo[]) || [];

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Header */}
			<div className="m-4 mb-0 flex items-center justify-between">
				<div className="flex items-center gap-2">
					<Typography className="text-lg leading-6 font-medium tracking-tight">
						Active Workers
					</Typography>
					<Chip
						label={`${workers.length} online`}
						size="small"
						variant="filled"
						color={workers.length > 0 ? 'success' : 'warning'}
					/>
				</div>
				<FuseSvgIcon className="text-green-500">lucide:cpu</FuseSvgIcon>
			</div>

			{/* Content */}
			{isLoading ? (
				<Box className="flex justify-center items-center h-48">
					<CircularProgress />
				</Box>
			) : workers.length === 0 ? (
				<Box className="flex flex-1 flex-col items-center justify-center p-8">
					<FuseSvgIcon className="text-gray-400" sx={{ fontSize: '48px', marginBottom: '16px' }}>
						lucide:alert-circle
					</FuseSvgIcon>
					<Typography color="textSecondary">No workers found</Typography>
				</Box>
			) : (
				<TableContainer sx={{ overflowX: 'auto' }}>
					<Table size="small" stickyHeader>
						<TableHead>
							<TableRow sx={{ backgroundColor: 'rgba(0, 0, 0, 0.04)' }}>
								<TableCell sx={{ fontWeight: 600 }}>Worker ID</TableCell>
								<TableCell sx={{ fontWeight: 600 }}>Hostname</TableCell>
								<TableCell sx={{ fontWeight: 600 }} align="center">
									Health
								</TableCell>
								<TableCell sx={{ fontWeight: 600 }} align="right">
									CPU %
								</TableCell>
								<TableCell sx={{ fontWeight: 600 }} align="right">
									Memory
								</TableCell>
								<TableCell sx={{ fontWeight: 600 }} align="right">
									Uptime
								</TableCell>
								<TableCell sx={{ fontWeight: 600 }} align="right">
									Queries
								</TableCell>
							</TableRow>
						</TableHead>
						<TableBody>
							{workers.map((worker) => (
								<TableRow key={worker.worker_id} hover>
									<TableCell>
										<Typography
											variant="body2"
											sx={{
												fontFamily: 'monospace',
												fontSize: '0.85rem'
											}}
											title={worker.worker_id}
										>
											{worker.worker_id.substring(0, 8)}...
										</Typography>
									</TableCell>
									<TableCell>
										<Typography variant="body2">
											{worker.hostname}
										</Typography>
									</TableCell>
									<TableCell align="center">
										<Chip
											label={worker.health_status}
											color={getHealthStatusColor(worker.health_status)}
											size="small"
											variant="filled"
										/>
									</TableCell>
									<TableCell align="right">
										<Typography
											variant="body2"
											sx={{
												color: worker.cpu_percent > 80 ? theme.palette.error.main : 'inherit'
											}}
										>
											{worker.cpu_percent.toFixed(1)}%
										</Typography>
									</TableCell>
									<TableCell align="right">
										<Typography
											variant="body2"
											sx={{
												fontSize: '0.85rem',
												color: (worker.memory_used_mb / worker.memory_total_mb) > 0.8
													? theme.palette.error.main
													: 'inherit'
											}}
											title={`${worker.memory_used_mb} / ${worker.memory_total_mb} MB`}
										>
											{formatMemory(worker.memory_used_mb, worker.memory_total_mb)}
										</Typography>
									</TableCell>
									<TableCell align="right">
										<Typography variant="body2">
											{formatUptime(worker.uptime_seconds)}
										</Typography>
									</TableCell>
									<TableCell align="right">
										<Typography variant="body2">
											{worker.active_queries}
											{worker.active_queries === 0 && (
												<Typography
													component="span"
													variant="caption"
													sx={{ display: 'block', color: 'textSecondary' }}
												>
													({worker.total_queries_processed} total)
												</Typography>
											)}
										</Typography>
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</TableContainer>
			)}
		</Paper>
	);
}

export default WorkersWidget;
