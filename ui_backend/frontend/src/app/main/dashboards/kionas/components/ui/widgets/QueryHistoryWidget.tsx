import Alert from '@mui/material/Alert';
import CircularProgress from '@mui/material/CircularProgress';
import Paper from '@mui/material/Paper';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';

import { useQueryHistoryData } from '../../../api/hooks/useQueryHistoryData';
import { formatDateTime, formatDurationMs } from '../../../lib/formatters';

function statusColor(status: string): 'success' | 'warning' | 'error' {
	if (status === 'Succeeded') {
		return 'success';
	}
	if (status === 'Running') {
		return 'warning';
	}
	return 'error';
}

function QueryHistoryWidget() {
	const { rows, isLoading, error } = useQueryHistoryData();

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load query history</Alert>
			</Paper>
		);
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">
					Query History
				</Typography>
			</div>
			<div className="p-4">
				{isLoading ? (
					<CircularProgress />
				) : (
					<TableContainer sx={{ maxHeight: 320 }}>
						<Table size="small" stickyHeader>
							<TableHead>
								<TableRow>
									<TableCell>Status</TableCell>
									<TableCell>Query</TableCell>
									<TableCell>Warehouse</TableCell>
									<TableCell align="right">Duration</TableCell>
									<TableCell>Timestamp</TableCell>
								</TableRow>
							</TableHead>
							<TableBody>
								{rows.map((row) => (
									<TableRow key={`${row.query_id}-${row.timestamp}`} hover>
										<TableCell>
											<Chip size="small" label={row.status} color={statusColor(row.status)} />
										</TableCell>
										<TableCell>{row.sql_digest}</TableCell>
										<TableCell>{row.warehouse_id || '-'}</TableCell>
										<TableCell align="right">{formatDurationMs(row.duration_ms || 0)}</TableCell>
										<TableCell>{formatDateTime(row.timestamp)}</TableCell>
									</TableRow>
								))}
								{rows.length === 0 && (
									<TableRow>
										<TableCell colSpan={5} align="center">
											<Typography color="text.secondary">No query history available</Typography>
										</TableCell>
									</TableRow>
								)}
							</TableBody>
						</Table>
					</TableContainer>
				)}
			</div>
		</Paper>
	);
}

export default QueryHistoryWidget;
