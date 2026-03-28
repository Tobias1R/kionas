import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

/**
 * Workers Widget - displays active workers in the cluster
 */
function WorkersWidget() {
	const { data, isLoading, error } = useDashboardData('workers');

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load workers</Alert>
			</Paper>
		);
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">Active Workers</Typography>
				<FuseSvgIcon className="text-green-500">lucide:cpu</FuseSvgIcon>
			</div>
			<div className="flex flex-1 flex-col p-4">
				{isLoading ? (
					<Box className="flex justify-center items-center h-32">
						<CircularProgress />
					</Box>
				) : data ? (
					<Box className="w-full">
						<pre className="text-xs overflow-auto max-h-48 bg-gray-900 text-gray-100 p-3 rounded">
							{JSON.stringify(data, null, 2)}
						</pre>
					</Box>
				) : (
					<Typography color="textSecondary">No workers found</Typography>
				)}
			</div>
		</Paper>
	);
}

export default WorkersWidget;
