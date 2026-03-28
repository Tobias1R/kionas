import '@i18n/i18n';
import './styles/index.css';
import { createRoot } from 'react-dom/client';
import { createBrowserRouter, RouterProvider } from 'react-router';
import routes from 'src/configs/routesConfig';
import { worker } from '@mock-utils/mswMockAdapter';
import { API_BASE_URL } from '@/utils/api';

async function mockSetup() {
	try {
		return await worker.start({
			onUnhandledRequest: 'bypass',
			serviceWorker: {
				url: `${API_BASE_URL}/mockServiceWorker.js`
			}
		});
	} catch (error) {
		console.warn('[MSW] Service Worker registration failed (this is okay in dev):', error);
		// Continue anyway - we're making real API calls to the backend
		return;
	}
}

/**
 * The root element of the application.
 */
const container = document.getElementById('app');

if (!container) {
	throw new Error('Failed to find the root element');
}

mockSetup().then(() => {
	/**
	 * The root component of the application.
	 */
	const root = createRoot(container, {
		onUncaughtError: (error, errorInfo) => {
			console.error('UncaughtError error', error, errorInfo.componentStack);
		},
		onCaughtError: (error, errorInfo) => {
			console.error('Caught error', error, errorInfo.componentStack);
		}
	});

	const router = createBrowserRouter(routes);

	root.render(<RouterProvider router={router} />);
});
