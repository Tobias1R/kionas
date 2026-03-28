import ky, { KyInstance } from 'ky';

// Determine API base URL
let apiBaseUrl: string;

if (import.meta.env.DEV) {
	// Development mode: Use VITE_API_BASE env var, or construct from current origin
	const viteApiBase = import.meta.env.VITE_API_BASE as string | undefined;
	
	if (viteApiBase) {
		// Docker or manual setup with environment variable
		apiBaseUrl = viteApiBase;
	} else {
		// Local dev: Use current origin (same host, port 8081 for backend)
		const currentHost = window.location.hostname;
		apiBaseUrl = `http://${currentHost}:8081`;
	}
} else {
	// Production: Relative path or env var
	apiBaseUrl = (import.meta.env.VITE_API_BASE_URL as string) || '/';
}

export const API_BASE_URL = apiBaseUrl;

let globalHeaders: Record<string, string> = {};

export const api: KyInstance = ky.create({
	prefixUrl: `${API_BASE_URL}/api`,
	hooks: {
		beforeRequest: [
			(request) => {
				Object.entries(globalHeaders).forEach(([key, value]) => {
					request.headers.set(key, value);
				});
			}
		]
	},
	retry: {
		limit: 2,
		methods: ['get', 'put', 'head', 'delete', 'options', 'trace']
	}
});

export const setGlobalHeaders = (headers: Record<string, string>) => {
	globalHeaders = { ...globalHeaders, ...headers };
};

export const removeGlobalHeaders = (headerKeys: string[]) => {
	headerKeys.forEach((key) => {
		delete globalHeaders[key];
	});
};

export const getGlobalHeaders = () => {
	return globalHeaders;
};

export default api;
