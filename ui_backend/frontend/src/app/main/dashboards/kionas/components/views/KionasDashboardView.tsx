'use client';
import FusePageSimple from '@fuse/core/FusePageSimple';
import { motion } from 'motion/react';
import Typography from '@mui/material/Typography';
import FuseLoading from '@fuse/core/FuseLoading';
import { useQueryClient } from '@tanstack/react-query';
import KionasDashboardHeader from '../ui/KionasDashboardHeader';
import ServerStatsWidget from '../ui/widgets/ServerStatsWidget';
import WorkersWidget from '../ui/widgets/WorkersWidget';
import SessionsWidget from '../ui/widgets/SessionsWidget';
import ConsulHealthWidget from '../ui/widgets/ConsulHealthWidget';
import QueryHistoryWidget from '../ui/widgets/QueryHistoryWidget';

const container = {
	show: {
		transition: {
			staggerChildren: 0.04
		}
	}
};

const item = {
	hidden: { opacity: 0, y: 20 },
	show: { opacity: 1, y: 0 }
};

/**
 * The Kionas dashboard app.
 */
function KionasDashboardView() {
	const queryClient = useQueryClient();
	const showQueryHistory = import.meta.env.VITE_KIONAS_ENABLE_QUERY_HISTORY === 'true';

	const handleRefresh = () => {
		void queryClient.invalidateQueries({ queryKey: ['dashboard'] });
	};

	return (
		<FusePageSimple
			header={<KionasDashboardHeader onRefresh={handleRefresh} />}
			content={
				<motion.div
					className="grid w-full grid-cols-1 gap-4 px-4 py-4 sm:grid-cols-2 md:px-8 lg:grid-cols-3"
					variants={container}
					initial="hidden"
					animate="show"
				>
					<motion.div
						variants={item}
						className="sm:col-span-2 lg:col-span-2"
					>
						<ServerStatsWidget />
					</motion.div>

					<motion.div
						variants={item}
						className="sm:col-span-2 lg:col-span-1"
					>
						<ConsulHealthWidget />
					</motion.div>

					<motion.div
						variants={item}
						className="sm:col-span-2 lg:col-span-3"
					>
						<SessionsWidget />
					</motion.div>

					<motion.div
						variants={item}
						className="sm:col-span-2 lg:col-span-3"
					>
						<WorkersWidget />
					</motion.div>

					{showQueryHistory && (
						<motion.div
							variants={item}
							className="sm:col-span-2 lg:col-span-3"
						>
							<QueryHistoryWidget />
						</motion.div>
					)}
				</motion.div>
			}
		/>
	);
}

export default KionasDashboardView;
