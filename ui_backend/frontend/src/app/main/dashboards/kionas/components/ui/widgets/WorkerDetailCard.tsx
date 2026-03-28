import Box from '@mui/material/Box';
import Divider from '@mui/material/Divider';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';

import { WorkerSystemInfo } from '../../../api/types/dashboard';
import {
	formatBytes,
	formatDateTime,
	formatDurationMs,
	formatNumber,
	formatUptime
} from '../../../lib/formatters';

interface WorkerDetailCardProps {
	worker: WorkerSystemInfo;
}

function DetailRow({ label, value }: { label: string; value: string }) {
	return (
		<Box>
			<Typography variant="caption" color="text.secondary">
				{label}
			</Typography>
			<Typography variant="body2" sx={{ fontWeight: 500 }}>
				{value}
			</Typography>
		</Box>
	);
}

function WorkerDetailCard({ worker }: WorkerDetailCardProps) {
	return (
		<Box sx={{ p: 2, bgcolor: 'background.default', borderRadius: 2 }}>
			<Grid container spacing={2}>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow label="Total Stages Executed" value={formatNumber(worker.total_stages_executed)} />
				</Grid>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow
						label="Total Partitions Executed"
						value={formatNumber(worker.total_partitions_executed)}
					/>
				</Grid>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow label="Total Rows Produced" value={formatNumber(worker.total_rows_produced)} />
				</Grid>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow label="Bytes Scanned" value={formatBytes(worker.bytes_scanned_total)} />
				</Grid>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow
						label="Stage Runtime Total"
						value={formatDurationMs(worker.total_stage_exec_ms)}
					/>
				</Grid>
				<Grid size={{ xs: 12, md: 4 }}>
					<DetailRow label="Uptime" value={formatUptime(worker.uptime_seconds)} />
				</Grid>
			</Grid>
			<Divider sx={{ my: 2 }} />
			<Grid container spacing={2}>
				<Grid size={{ xs: 12, md: 6 }}>
					<DetailRow label="Started At" value={formatDateTime(worker.started_at)} />
				</Grid>
				<Grid size={{ xs: 12, md: 6 }}>
					<DetailRow label="Updated At" value={formatDateTime(worker.updated_at)} />
				</Grid>
			</Grid>
		</Box>
	);
}

export default WorkerDetailCard;
