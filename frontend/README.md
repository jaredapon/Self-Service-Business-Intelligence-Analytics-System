# Capstone Frontend

This is the **React frontend** for the Capstone Project:  
**Data-Driven Product Bundling Strategy and Pricing Optimization Using Market Basket Analysis and Prescriptive Analytics**

The frontend provides:
- 🔑 **Login Page** using Keycloak (OpenID Connect authentication).  
- 📂 **File Upload** for sales and transaction datasets to MinIO.  
- 📊 **Embedded Dash App** for interactive analytics (e.g., Market Basket Analysis, Price Elasticity of Demand, Exponential Smoothing).  
- 📡 **API Integration** with the backend ETL/ML services running in Docker Compose.  

---

## 📦 Folder Structure

```
frontend/
├─ public/            # static assets
├─ src/
│  ├─ components/     # reusable UI components
│  ├─ pages/          # Login, Dashboard, Upload
│  ├─ services/       # Keycloak and API helpers
│  ├─ App.tsx         # main app component
│  └─ main.tsx        # entrypoint
├─ .env               # environment variables
├─ package.json
└─ README.md
```

---

## ⚙️ Environment Variables

Create a `.env` file in the `frontend/` directory:

```ini
# Backend API
VITE_API_BASE_URL=http://localhost:8000

# Power BI Dashboard
VITE_POWER_BI_EMBED_URL=https://app.powerbi.com/view?r=YOUR_REPORT_ID

# Keycloak Authentication (Optional)
VITE_KEYCLOAK_URL=http://localhost:8080
VITE_KEYCLOAK_REALM=capstone
VITE_KEYCLOAK_CLIENT_ID=frontend

# Optional Services
VITE_DASH_URL=http://localhost:8050
VITE_MINIO_CONSOLE=http://localhost:9001
```

**Notes:**
- If you don't set `VITE_API_BASE_URL`, it defaults to `http://localhost:8000`
- Leave `VITE_POWER_BI_EMBED_URL` empty to show a placeholder on the dashboard

---

## Development

Install dependencies:

```
npm install
```

Start the Vite dev server:

```
npm run dev
```

Visit the app:

http://localhost:5173

---

## 📊 Power BI Dashboard

The main dashboard displays an embedded Power BI report for interactive analytics visualization.

### Setup Instructions:

1. **Create your Power BI report** in Power BI Desktop
2. **Publish to Power BI Service**
3. **Get the embed URL:**
   - Open your report in Power BI Service
   - Click **File → Embed report → Publish to web**
   - Copy the iframe `src` URL
4. **Configure the frontend:**
   - Add to `.env` file: `VITE_POWER_BI_EMBED_URL=your_url_here`
   - Or update `frontend/src/config.ts` directly
5. **Restart the dev server** to see your dashboard

**Note:** If no URL is configured, a helpful placeholder with instructions will be displayed.

---

## 📤 File Upload Feature

The Data Upload page allows you to upload sales data files to trigger the ETL pipeline:

### Supported Files:
- **Raw Sales Files**: Transaction-level sales data (must contain "Sales Transaction List" in filename)
- **Sales by Product Files**: Product-level aggregated sales (must contain "Sales Report by Product" in filename)

### Supported Formats:
- CSV (.csv)
- Excel (.xls, .xlsx)

### Upload Flow:
1. Drag & drop or select files for both categories
2. Review selected files
3. Click "Upload" to confirm
4. Files are uploaded to MinIO backend
5. ETL pipeline is automatically triggered
6. Data is processed and loaded into PostgreSQL
7. Landing bucket is automatically cleaned after successful processing

### Requirements:
- Both Raw Sales and Sales by Product files are required
- Maximum 24 files per category
- Maximum 200MB per file

---

## 🔧 Development with Backend

The frontend uses `vite.config.ts` proxy to forward API requests to the backend during development:

```typescript
proxy: {
  '/upload': {
    target: 'http://localhost:8000',
    changeOrigin: true,
  },
}
```

**To run full stack locally:**

1. Start backend API: `python backend/run_api.py`
2. Start observer: `python backend/run_observer.py`
3. Start frontend: `npm run dev` (from `frontend/` directory)
4. Upload files through the UI at `http://localhost:5173`
