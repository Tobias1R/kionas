'use client';
import FusePageSimple from '@fuse/core/FusePageSimple';
import { motion } from 'motion/react';
import Typography from '@mui/material/Typography';
import FuseLoading from '@fuse/core/FuseLoading';
import KionasDashboardHeader from '../ui/KionasDashboardHeader';
import ServerStatsWidget from '../ui/widgets/ServerStatsWidget';
import WorkersWidget from '../ui/widgets/WorkersWidget';
import SessionsWidget from '../ui/widgets/SessionsWidget';
import ConsulHealthWidget from '../ui/widgets/ConsulHealthWidget';

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
	return (
		<FusePageSimple
			header={<KionasDashboardHeader />}
			content={
				<motion.div
					className="grid w-full grid-cols-1 gap-4 px-4 py-4 sm:grid-cols-2 md:px-8 lg:grid-cols-2"
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
						className="sm:col-span-1 lg:col-span-1"
					>
						<SessionsWidget />
					</motion.div>

					<motion.div
						variants={item}
						className="sm:col-span-1 lg:col-span-1"
					>
						<ConsulHealthWidget />
					</motion.div>

					<motion.div
						variants={item}
						className="sm:col-span-2 lg:col-span-2"
					>
						<WorkersWidget />
					</motion.div>
				</motion.div>
			}
		/>
	);
}

export default KionasDashboardView;
