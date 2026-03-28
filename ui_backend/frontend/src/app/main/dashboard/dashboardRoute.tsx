import React from 'react';
import { FuseRouteConfigType } from '@fuse/utils/FuseUtils';
import DashboardPage from './DashboardPage';

const dashboardRouteConfig: FuseRouteConfigType = {
	routes: [
		{
			path: 'example',
			element: <DashboardPage />
		},
		{
			path: 'dashboard',
			element: <DashboardPage />
		}
	]
};

export default dashboardRouteConfig;
