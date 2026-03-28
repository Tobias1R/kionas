import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

/**
 * Sessions Widget - displays active session count and info
 */
function SessionsWidget() {
	const { data, isLoading, error } = useDashboardData('sessions');
	const count = (data as any)?.count || 0;

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load sessions</Alert>
			</Paper>
		);
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">Active Sessions</Typography>
				<FuseSvgIcon className="text-purple-500">lucide:users</FuseSvgIcon>
			</div>
			<div className="flex flex-1 flex-col items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : (
					<Box className="text-center">
						<Typography className="text-5xl font-bold">{count}</Typography>
						<Typography className="text-sm text-gray-500 mt-2">sessions active</Typography>
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

export default SessionsWidget;
