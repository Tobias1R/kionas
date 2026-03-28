import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';

import { WorkerSystemInfo } from '../../../api/types/dashboard';

interface WorkerMetricChipsProps {
	worker: WorkerSystemInfo;
}

function WorkerMetricChips({ worker }: WorkerMetricChipsProps) {
	return (
		<Stack
			direction="row"
			spacing={1}
			useFlexGap
			flexWrap="wrap"
		>
			<Chip size="small" label={`Stages active: ${worker.active_stages}`} />
			<Chip size="small" label={`Partitions active: ${worker.active_partitions}`} />
			<Chip size="small" label={`CPU: ${worker.cpu_percent.toFixed(1)}%`} />
			<Chip size="small" label={`Threads: ${worker.thread_count}`} />
		</Stack>
	);
}

export default WorkerMetricChips;
