import React from 'react';
import {
	Container,
	Paper,
	Typography,
	Box,
	Card,
	CardContent,
	CircularProgress,
	Alert
} from '@mui/material';
import { useQuery } from '@tanstack/react-query';

/**
 * Fetch data from backend dashboard API
 */
async function fetchDashboardData(name: string) {
	try {
		const response = await fetch(`/dashboard/key?name=${encodeURIComponent(name)}`);
		if (!response.ok) {
			throw new Error(`API error: ${response.status}`);
		}
		return response.json();
	} catch (error) {
		console.error(`Failed to fetch ${name}:`, error);
		throw error;
	}
}

/**
 * Data Panel Component
 */
function DataPanel({ title, name }: { title: string; name: string }) {
	const { data, isLoading, error } = useQuery({
		queryKey: ['dashboard', name],
		queryFn: () => fetchDashboardData(name),
		refetchInterval: 15000 // Refresh every 15 seconds
	});

	return (
		<Card sx={{ height: '100%' }}>
			<CardContent>
				<Typography color="textSecondary" gutterBottom>
					{title}
				</Typography>
				{isLoading && <CircularProgress size={40} />}
				{error && <Typography color="error">Error loading data</Typography>}
				{data && (
					<Box
						component="pre"
						sx={{
							bgcolor: '#f5f5f5',
							p: 2,
							borderRadius: 1,
							overflow: 'auto',
							maxHeight: 200,
							fontSize: '0.75rem'
						}}
					>
						{JSON.stringify(data, null, 2)}
					</Box>
				)}
			</CardContent>
		</Card>
	);
}

/**
 * Dashboard Page
 */
function DashboardPage() {
	return (
		<Container maxWidth="lg" sx={{ py: 4 }}>
			<Typography variant="h4" component="h1" gutterBottom sx={{ mb: 4 }}>
				Kionas Dashboard
			</Typography>

			<Alert severity="info" sx={{ mb: 4 }}>
				Development mode: Demo user authenticated. All widgets refresh every 15 seconds.
			</Alert>

			<Box
				sx={{
					display: 'grid',
					gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' },
					gap: 3
				}}
			>
				<Box>
					<DataPanel title="Server Stats" name="server_stats" />
				</Box>

				<Box>
					<DataPanel title="Sessions" name="sessions" />
				</Box>

				<Box>
					<DataPanel title="Tokens" name="tokens" />
				</Box>

				<Box>
					<DataPanel title="Workers" name="workers" />
				</Box>

				<Box sx={{ gridColumn: { xs: 'span 1', md: 'span 2' } }}>
					<DataPanel title="Consul Cluster Summary" name="consul_cluster_summary" />
				</Box>
			</Box>

			<Paper sx={{ mt: 4, p: 3, bgcolor: '#f9f9f9' }}>
				<Typography variant="h6" gutterBottom>
					API Status
				</Typography>
				<Typography variant="body2" color="textSecondary">
					Backend: <code>http://localhost:8081</code>
				</Typography>
				<Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
					Frontend: <code>http://localhost:5173</code>
				</Typography>
				<Typography variant="body2" color="textSecondary" sx={{ mt: 2 }}>
					💡 <strong>Tip:</strong> Edit this file in{' '}
					<code>src/app/main/dashboard/DashboardPage.tsx</code> and save to see changes instantly.
				</Typography>
			</Paper>
		</Container>
	);
}

export default DashboardPage;
