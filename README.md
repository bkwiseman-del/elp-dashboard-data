# Trucksafe ELP Violation Dashboard - Data Pipeline

Automated data pipeline that fetches real FMCSA English Language Proficiency violation data weekly and updates the dashboard.

## 🎯 Overview

This system automatically:
- ✅ Fetches ELP violation data from FMCSA via Socrata API
- ✅ Processes and aggregates data by state and month
- ✅ Updates every Monday automatically (GitHub Actions)
- ✅ Costs $0/month (100% free)

## 📋 Setup Instructions

### Step 1: Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `elp-dashboard-data` (or your choice)
3. Make it **Public** (required for GitHub Pages)
4. Click **"Create repository"**

### Step 2: Upload Files

Upload these files to your new repository:

```
elp-dashboard-data/
├── .github/
│   └── workflows/
│       └── weekly-update.yml    # GitHub Actions workflow
├── update_data.py               # Python data fetcher script
├── index.html                   # Dashboard HTML
└── README.md                    # This file
```

**Option A: Via GitHub Website**
1. Click **"Add file" → "Upload files"**
2. Drag all files into the upload area
3. Click **"Commit changes"**

**Option B: Via Git Command Line**
```bash
git clone https://github.com/YOUR_USERNAME/elp-dashboard-data.git
cd elp-dashboard-data
# Copy all files into this directory
git add .
git commit -m "Initial commit - ELP dashboard data pipeline"
git push
```

### Step 3: Enable GitHub Actions

1. Go to your repository on GitHub
2. Click **"Actions"** tab
3. Click **"I understand my workflows, go ahead and enable them"**

### Step 4: Enable GitHub Pages

1. Go to **Settings** → **Pages**
2. Under **"Source"**, select **"Deploy from a branch"**
3. Select branch: **main** (or **master**)
4. Select folder: **/ (root)**
5. Click **"Save"**

Your dashboard will be live at:
```
https://YOUR_USERNAME.github.io/elp-dashboard-data/
```

### Step 5: Run First Data Update

1. Go to **"Actions"** tab
2. Click **"Update ELP Violation Data"** workflow
3. Click **"Run workflow"** button
4. Click **"Run workflow"** (green button)

This will fetch the first batch of data immediately!

## 🔄 How It Works

### Automatic Weekly Updates

Every **Monday at 1:00 AM EST**, GitHub Actions automatically:

1. Runs `update_data.py` script
2. Fetches latest FMCSA violation data from Socrata API
3. Processes and aggregates data
4. Saves to `elp_data.json`
5. Commits and pushes changes
6. GitHub Pages automatically deploys the update

### Manual Updates

You can trigger an update anytime:

1. Go to **Actions** tab
2. Click **"Update ELP Violation Data"**
3. Click **"Run workflow"**

## 📊 Data Source

**FMCSA Motor Carrier Management Information System (MCMIS)**
- URL: https://data.transportation.gov/Roadway-Safety/Motor-Carrier-Management-Information-System-MCMIS-/8mt8-2mdr
- API: Socrata Open Data API
- No API key required for basic usage
- Updates: FMCSA updates this dataset regularly

**What We Fetch:**
- Violation Code: `391.11B2` (English Language Proficiency)
- Date Range: Since June 25, 2025 (OOS restoration date)
- Data Points: Inspection date, state, OOS indicator

## 🛠️ Customization

### Change Update Frequency

Edit `.github/workflows/weekly-update.yml`:

```yaml
schedule:
  # Daily at 6 AM UTC
  - cron: '0 6 * * *'
  
  # Every Monday and Friday at 6 AM UTC
  - cron: '0 6 * * 1,5'
  
  # First day of each month at 6 AM UTC
  - cron: '0 6 1 * *'
```

### Modify Data Processing

Edit `update_data.py` to change:
- Aggregation logic
- Date ranges
- Additional calculations
- Output format

## 📁 Output Data Structure

`elp_data.json` contains:

```json
{
  "last_updated": "February 13, 2026",
  "total_oos": 527,
  "total_all": 1247,
  "oos_rate": 42.3,
  "avg_per_month": 67,
  "peak_month": "Oct '25",
  "peak_count": 73,
  "mom_change": -4.4,
  "monthly": {
    "labels": ["Jun 25", "Jul 25", ...],
    "oos": [58, 61, 65, ...],
    "all": [142, 148, 156, ...]
  },
  "states": [
    {"state": "CA", "oos": 186, "all": 438},
    {"state": "TX", "oos": 171, "all": 402},
    ...
  ],
  "biggest_movers": {
    "increases": [...],
    "decreases": [...]
  },
  "state_count": 15
}
```

## 🔧 Troubleshooting

### Workflow Not Running

**Check:**
1. Actions are enabled (Settings → Actions → Allow all actions)
2. Workflow file is in `.github/workflows/` directory
3. File has `.yml` extension
4. Repository is public (private repos have limited free Actions minutes)

### Data Not Updating

**Check:**
1. Go to Actions tab → View latest workflow run
2. Look for error messages in the logs
3. Common issues:
   - API connectivity
   - Rate limiting (unlikely with Socrata)
   - No new violations (might be expected)

### Dashboard Not Loading Data

**Check:**
1. `elp_data.json` exists in repository
2. GitHub Pages is enabled and deployed
3. Check browser console for errors (F12)
4. Verify JSON file URL is accessible

## 📞 Support

For issues or questions:
- **Trucksafe Consulting:** https://www.trucksafe.com/contact
- **GitHub Issues:** Create an issue in your repository

## 📜 License

This data pipeline is provided by Trucksafe Consulting, LLC for use with the ELP Violation Analytics Dashboard.

---

**Last Updated:** February 2026  
**Version:** 1.0.0
