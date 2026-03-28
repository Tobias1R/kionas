import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import Chip from '@mui/material/Chip';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

/**
 * Consul Health Widget - displays cluster health status
 */
function ConsulHealthWidget() {
	const { data, isLoading, error } = useDashboardData('consul_cluster_summary');
	const status = (data as any)?.status || 'unknown';

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load Consul health</Alert>
			</Paper>
		);
	}

	const statusColor = status === 'passing' ? 'success' : status === 'warning' ? 'warning' : 'error';

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">Consul Health</Typography>
				<FuseSvgIcon className="text-orange-500">lucide:heart-pulse</FuseSvgIcon>
			</div>
			<div className="flex flex-1 flex-col items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : (
					<Box className="text-center">
						<Chip
							label={status.toUpperCase()}
							color={statusColor}
							variant="filled"
							className="mb-4"
						/>
						<Box className="mt-4">
							<pre className="text-xs overflow-auto max-h-32 bg-gray-900 text-gray-100 p-2 rounded">
								{JSON.stringify(data, null, 2)}
							</pre>
						</Box>
					</Box>
				)}
			</div>
		</Paper>
	);
}

export default ConsulHealthWidget;
