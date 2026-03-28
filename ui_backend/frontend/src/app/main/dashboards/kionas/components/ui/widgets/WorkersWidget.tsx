import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Chip from '@mui/material/Chip';
import Accordion from '@mui/material/Accordion';
import AccordionDetails from '@mui/material/AccordionDetails';
import AccordionSummary from '@mui/material/AccordionSummary';
import Grid from '@mui/material/Grid';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useTheme } from '@mui/material/styles';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import { useWorkersData } from '../../../api/hooks/useWorkersData';
import { WorkerSystemInfo } from '../../../api/types/dashboard';
import { formatNumber, formatUptime } from '../../../lib/formatters';
import WorkerDetailCard from './WorkerDetailCard';
import WorkerMetricChips from './WorkerMetricChips';

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

function WorkerSummaryCard({ worker }: { worker: WorkerSystemInfo }) {
	const memoryPercent = worker.memory_total_mb > 0 ? (worker.memory_used_mb / worker.memory_total_mb) * 100 : 0;

	return (
		<Accordion disableGutters>
			<AccordionSummary expandIcon={<ExpandMoreIcon />}>
				<Box sx={{ width: '100%' }}>
					<Grid container spacing={2} alignItems="center">
						<Grid size={{ xs: 12, md: 3 }}>
							<Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
								{worker.hostname}
							</Typography>
							<Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'monospace' }}>
								{worker.worker_id}
							</Typography>
						</Grid>
						<Grid size={{ xs: 6, md: 2 }}>
							<Chip size="small" label={worker.health_status} color={getHealthStatusColor(worker.health_status)} />
						</Grid>
						<Grid size={{ xs: 6, md: 2 }}>
							<Typography variant="body2">CPU: {worker.cpu_percent.toFixed(1)}%</Typography>
							<Typography variant="caption" color="text.secondary">
								Memory: {memoryPercent.toFixed(1)}%
							</Typography>
						</Grid>
						<Grid size={{ xs: 6, md: 2 }}>
							<Typography variant="body2">Pool: {worker.warehouse_pool || '-'}</Typography>
							<Typography variant="caption" color="text.secondary">Uptime: {formatUptime(worker.uptime_seconds)}</Typography>
						</Grid>
						<Grid size={{ xs: 12, md: 3 }}>
							<WorkerMetricChips worker={worker} />
						</Grid>
					</Grid>
				</Box>
			</AccordionSummary>
			<AccordionDetails>
				<WorkerDetailCard worker={worker} />
			</AccordionDetails>
		</Accordion>
	);
}

/**
 * Workers Widget - displays active workers in the cluster
 */
function WorkersWidget() {
	const { workers, isLoading, error } = useWorkersData();
	const theme = useTheme();

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load workers</Alert>
			</Paper>
		);
	}

	const healthyWorkers = workers.filter((worker) => worker.health_status === 'Healthy').length;

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Header */}
			<div className="m-4 mb-0 flex items-center justify-between">
				<div className="flex items-center gap-2">
					<Typography className="text-lg leading-6 font-medium tracking-tight">
						Workers Execution
					</Typography>
					<Chip
						label={`${workers.length} online`}
						size="small"
						variant="filled"
						color={workers.length > 0 ? 'success' : 'warning'}
					/>
					<Chip
						label={`${healthyWorkers}/${workers.length} healthy`}
						size="small"
						variant="outlined"
						color={healthyWorkers === workers.length ? 'success' : 'warning'}
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
				<Box sx={{ p: 2, display: 'grid', gap: 1.5 }}>
					{workers.map((worker) => (
						<WorkerSummaryCard key={worker.worker_id} worker={worker} />
					))}

					<Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', pt: 1 }}>
						<Typography variant="caption" color="text.secondary">
							Total rows produced: {formatNumber(workers.reduce((sum, worker) => sum + worker.total_rows_produced, 0))}
						</Typography>
						<Typography variant="caption" color="text.secondary">
							Total stages executed: {formatNumber(workers.reduce((sum, worker) => sum + worker.total_stages_executed, 0))}
						</Typography>
					</Box>
				</Box>
			)}
		</Paper>
	);
}

export default WorkersWidget;
