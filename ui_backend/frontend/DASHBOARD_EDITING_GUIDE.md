# Kionas Dashboard - Files to Edit Guide

## Dashboard Structure Overview

Your dashboard follows the **Fuse React design pattern** with this folder structure:

```
src/app/main/dashboards/kionas/
├── route.tsx                          # Route configuration (path & lazy loading)
├── api/
│   └── hooks/
│       └── useDashboardData.ts         # ⭐ Data fetching hook - connects to backend
├── components/
│   ├── views/
│   │   └── KionasDashboardView.tsx    # Main dashboard layout (grid, animations)
│   └── ui/
│       ├── KionasDashboardHeader.tsx   # Title, breadcrumbs, buttons
│       └── widgets/                   # Individual data display cards
│           ├── ServerStatsWidget.tsx  # Server stats
│           ├── SessionsWidget.tsx     # Active sessions count
│           ├── WorkersWidget.tsx      # Workers list
│           └── ConsulHealthWidget.tsx # Consul cluster health
```

---

## 1️⃣ Backend Data Connection

**File to edit:** `src/app/main/dashboards/kionas/api/hooks/useDashboardData.ts`

This hook fetches data from your backend. Currently it uses this endpoint:
```
GET /dashboard/key?name={key}
```

**Customize the API endpoint:**

```typescript
export function useDashboardData(key: string) {
	return useQuery({
		queryKey: ['dashboard', key],
		queryFn: async () => {
			// ⭐ Change the API endpoint here:
			const response = await fetch(`${API_BASE}/key?name=${encodeURIComponent(key)}`);
			if (!response.ok) {
				throw new Error(`Failed to fetch ${key}: ${response.statusText}`);
			}
			return response.json() as Promise<DashboardData>;
		},
		refetchInterval: 15000, // ⭐ Change refresh interval (ms) here
		retry: 1
	});
}
```

**Available data keys being fetched:**
- `'server_stats'` → used by ServerStatsWidget
- `'sessions'` → used by SessionsWidget  
- `'workers'` → used by WorkersWidget
- `'consul_cluster_summary'` → used by ConsulHealthWidget

---

## 2️⃣ Dashboard Layout (Main View)

**File to edit:** `src/app/main/dashboards/kionas/components/views/KionasDashboardView.tsx`

This file controls:
- Which widgets appear
- Grid layout (1 col mobile, 2 col tablet, 3 col desktop)
- Animation effects
- Widget ordering

**Example: Add a new widget**

```typescript
// 1. Import the new widget
import NewMetricWidget from '../ui/widgets/NewMetricWidget';

// 2. Add it to the grid
<motion.div
	variants={item}
	className="sm:col-span-2 lg:col-span-1"  // Size control
>
	<NewMetricWidget />
</motion.div>
```

**Grid column sizes:**
- `className="sm:col-span-1 lg:col-span-1"` → Small widget (1 col on desktop)
- `className="sm:col-span-2 lg:col-span-1"` → Medium widget
- `className="sm:col-span-3 lg:col-span-3"` → Full-width widget

---

## 3️⃣ Header (Title & Controls)

**File to edit:** `src/app/main/dashboards/kionas/components/ui/KionasDashboardHeader.tsx`

This controls:
- Dashboard title
- Subtitle
- Action buttons (Refresh, Settings, etc.)

**Example: Add a button**

```typescript
<Button
	startIcon={<FuseSvgIcon>lucide:download</FuseSvgIcon>}
	variant="contained"
	color="primary"
	onClick={() => { /* your action */ }}
>
	Export Data
</Button>
```

---

## 4️⃣ Individual Widgets

**Files to edit:** `src/app/main/dashboards/kionas/components/ui/widgets/*.tsx`

Each widget is independent. They all follow this pattern:

```typescript
function MyWidget() {
	// Fetch data using the hook
	const { data, isLoading, error } = useDashboardData('my_data_key');

	if (error) {
		return <Alert severity="error">Failed to load data</Alert>;
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			{/* Widget content here */}
		</Paper>
	);
}
```

**What each widget currently displays:**

| Widget | Data Key | Shows |
|--------|----------|-------|
| **ServerStatsWidget** | `server_stats` | JSON data in code block |
| **SessionsWidget** | `sessions` | Large count display + JSON |
| **WorkersWidget** | `workers` | JSON data in code block |
| **ConsulHealthWidget** | `consul_cluster_summary` | Status chip + JSON |

---

## 5️⃣ Adding a New Widget

**Step 1: Create the widget file**
```
src/app/main/dashboards/kionas/components/ui/widgets/MyNewWidget.tsx
```

**Step 2: Use the template**
```typescript
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import FuseSvgIcon from '@fuse/core/FuseSvgIcon';
import { useDashboardData } from '../../../api/hooks/useDashboardData';

function MyNewWidget() {
	const { data, isLoading, error } = useDashboardData('my_data_key');

	if (error) {
		return (
			<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
				<Alert severity="error" className="m-4">Failed to load data</Alert>
			</Paper>
		);
	}

	return (
		<Paper className="flex flex-auto flex-col overflow-hidden rounded-xl shadow-sm">
			<div className="m-4 mb-0 flex items-start justify-between">
				<Typography className="truncate text-lg leading-6 font-medium tracking-tight">
					My Widget Title
				</Typography>
				<FuseSvgIcon className="text-blue-500">lucide:icon-name</FuseSvgIcon>
			</div>
			<div className="flex flex-1 items-center justify-center p-8">
				{isLoading ? (
					<CircularProgress />
				) : data ? (
					// Display your data here
					<Typography>{JSON.stringify(data)}</Typography>
				) : (
					<Typography color="textSecondary">No data available</Typography>
				)}
			</div>
		</Paper>
	);
}

export default MyNewWidget;
```

**Step 3: Import in KionasDashboardView.tsx**
```typescript
import MyNewWidget from '../ui/widgets/MyNewWidget';
```

**Step 4: Add to grid**
```typescript
<motion.div variants={item} className="sm:col-span-2 lg:col-span-1">
	<MyNewWidget />
</motion.div>
```

---

## 6️⃣ Styling

All components use **Tailwind CSS** (utility classes) + **Material-UI components**:

```typescript
// Tailwind classes
<div className="m-4 p-8 flex items-center justified-center">

// Material-UI components
<Paper> <Typography> <Button> <Alert> <Chip> <CircularProgress>

// Lucide icons
<FuseSvgIcon className="text-blue-500">lucide:icon-name</FuseSvgIcon>
```

**Available icon names:** `lucide:server`, `lucide:users`, `lucide:cpu`, `lucide:heart-pulse`, etc.

---

## 7️⃣ Access the Dashboard

Once you're in the dev environment:

```
http://localhost:5173/dashboard
```

The route is defined in `route.tsx`:
```typescript
const route: FuseRouteItemType = {
	path: 'dashboard',  // ⭐ URL path
	element: <KionasDashboardView />
};
```

---

## Summary: What to Edit

| Goal | File to Edit |
|------|--------------|
| Change backend API endpoint | `api/hooks/useDashboardData.ts` |
| Add/remove widgets | `components/views/KionasDashboardView.tsx` |
| Change title or buttons | `components/ui/KionasDashboardHeader.tsx` |
| Modify existing widget | `components/ui/widgets/*.tsx` (any widget) |
| Create new widget | Create new `.tsx` file in `widgets/` folder |
| Change refresh interval | `api/hooks/useDashboardData.ts` (refetchInterval) |
| Change grid layout | `components/views/KionasDashboardView.tsx` (className) |

---

## Next Steps

1. **Test the dashboard:** `http://localhost:5173/dashboard`
2. **Create backend endpoint:** Implement `/dashboard/key?name={key}` if not already done
3. **Add real data:** Return actual data structures from backend
4. **Customize widgets:** Modify display format based on your data
5. **Add more widgets:** Follow the template to create new metrics
