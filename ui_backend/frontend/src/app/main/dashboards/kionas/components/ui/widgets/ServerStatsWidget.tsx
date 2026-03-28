import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

/**
 * Server Stats Widget - displays current server statistics
 */
function ServerStatsWidget() {
	const { data, isLoading, error } = useDashboardData('server_stats');

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load server stats</Alert>
			</Paper>
		);
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">Server Stats</Typography>
				<FuseSvgIcon className="text-blue-500">lucide:server</FuseSvgIcon>
			</div>
			<div className="flex flex-1 items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : data ? (
					<Box className="w-full">
						<pre className="text-xs overflow-auto max-h-48 bg-gray-900 text-gray-100 p-3 rounded">
							{JSON.stringify(data, null, 2)}
						</pre>
					</Box>
				) : (
					<Typography color="textSecondary">No data available</Typography>
				)}
			</div>
		</Paper>
	);
}

export default ServerStatsWidget;
