import { lazy } from 'react';
import { FuseRouteItemType } from '@fuse/utils/FuseUtils';

const KionasDashboardView = lazy(() => import('./components/views/KionasDashboardView'));

/**
 * The Kionas Dashboard Route
 */
const route: FuseRouteItemType = {
	path: 'dashboard',
	element: <KionasDashboardView />
};

export default route;
