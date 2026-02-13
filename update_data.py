#!/usr/bin/env python3
"""
FMCSA ELP Violation Data Fetcher
Fetches English Language Proficiency violation data from FMCSA via Socrata API
Processes and aggregates data for the Trucksafe ELP Dashboard
"""

import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
import sys

# Socrata API endpoint for FMCSA MCMIS Inspections
SOCRATA_API_URL = "https://data.transportation.gov/resource/u4mh-kwnx.json"

# ELP violation code in FMCSA system
# 391.11(b)(2) - Driver cannot read/speak English sufficiently
ELP_VIOLATION_CODE = "391.11B2"

# Date when OOS criteria was restored
OOS_RESTORATION_DATE = "2025-06-25"

def fetch_elp_violations(limit=50000, offset=0):
    """
    Fetch ELP violations from FMCSA Socrata API
    
    Args:
        limit: Maximum number of records to fetch per request
        offset: Starting position for pagination
    
    Returns:
        List of violation records
    """
    print(f"Fetching ELP violations from FMCSA (offset: {offset})...")
    
    # Build query parameters
    # Filter for ELP violations since restoration date
    params = {
        "$where": f"inspection_date >= '{OOS_RESTORATION_DATE}' AND violation_code = '{ELP_VIOLATION_CODE}'",
        "$limit": limit,
        "$offset": offset,
        "$order": "inspection_date DESC",
        "$select": "inspection_date, state, violation_code, oos_ind, driver_oos_total"
    }
    
    try:
        response = requests.get(SOCRATA_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✓ Fetched {len(data)} records")
        return data
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching data: {e}")
        return []

def fetch_all_elp_violations():
    """
    Fetch all ELP violations using pagination
    
    Returns:
        List of all violation records
    """
    all_violations = []
    offset = 0
    limit = 50000
    
    while True:
        batch = fetch_elp_violations(limit=limit, offset=offset)
        if not batch:
            break
        
        all_violations.extend(batch)
        
        # If we got less than the limit, we've reached the end
        if len(batch) < limit:
            break
        
        offset += limit
    
    print(f"\n✓ Total ELP violations fetched: {len(all_violations)}")
    return all_violations

def process_violations(violations):
    """
    Process and aggregate violation data
    
    Args:
        violations: List of raw violation records
    
    Returns:
        Dictionary with aggregated data
    """
    print("\nProcessing violations...")
    
    # Initialize data structures
    monthly_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_monthly = defaultdict(lambda: defaultdict(lambda: {"oos": 0, "all": 0}))
    
    total_oos = 0
    total_all = 0
    
    for violation in violations:
        try:
            # Parse inspection date
            inspection_date = violation.get("inspection_date", "")
            if not inspection_date:
                continue
            
            # Extract year-month
            date_obj = datetime.fromisoformat(inspection_date.split("T")[0])
            year_month = date_obj.strftime("%Y-%m")
            month_label = date_obj.strftime("%b %y")
            
            # Get state
            state = violation.get("state", "UNKNOWN")
            
            # Check if OOS
            is_oos = violation.get("oos_ind") == "Y" or violation.get("driver_oos_total", "0") != "0"
            
            # Increment counters
            monthly_data[year_month]["all"] += 1
            total_all += 1
            
            state_data[state]["all"] += 1
            state_monthly[state][year_month]["all"] += 1
            
            if is_oos:
                monthly_data[year_month]["oos"] += 1
                total_oos += 1
                state_data[state]["oos"] += 1
                state_monthly[state][year_month]["oos"] += 1
        
        except Exception as e:
            print(f"Warning: Error processing violation: {e}")
            continue
    
    # Sort monthly data
    sorted_months = sorted(monthly_data.keys())
    
    # Prepare monthly arrays for charts
    monthly_labels = []
    monthly_oos = []
    monthly_all = []
    
    for month in sorted_months:
        date_obj = datetime.strptime(month, "%Y-%m")
        label = date_obj.strftime("%b %y")
        monthly_labels.append(label)
        monthly_oos.append(monthly_data[month]["oos"])
        monthly_all.append(monthly_data[month]["all"])
    
    # Get top 10 states
    top_states = sorted(
        state_data.items(),
        key=lambda x: x[1]["oos"],
        reverse=True
    )[:10]
    
    # Calculate month-over-month changes
    biggest_movers = calculate_biggest_movers(state_monthly, sorted_months)
    
    # Calculate statistics
    avg_per_month = total_oos / len(sorted_months) if sorted_months else 0
    peak_month = max(monthly_data.items(), key=lambda x: x[1]["oos"]) if monthly_data else (None, {"oos": 0})
    peak_month_label = datetime.strptime(peak_month[0], "%Y-%m").strftime("%b '%y") if peak_month[0] else "N/A"
    
    # Calculate month-over-month percentage
    if len(monthly_oos) >= 2:
        current_month = monthly_oos[-1]
        last_month = monthly_oos[-2]
        mom_change = ((current_month - last_month) / last_month * 100) if last_month > 0 else 0
    else:
        mom_change = 0
    
    # Get last update date
    last_update = datetime.now().strftime("%B %d, %Y")
    
    result = {
        "last_updated": last_update,
        "total_oos": total_oos,
        "total_all": total_all,
        "oos_rate": round((total_oos / total_all * 100) if total_all > 0 else 0, 1),
        "avg_per_month": round(avg_per_month),
        "peak_month": peak_month_label,
        "peak_count": peak_month[1]["oos"],
        "mom_change": round(mom_change, 1),
        "monthly": {
            "labels": monthly_labels,
            "oos": monthly_oos,
            "all": monthly_all
        },
        "states": [
            {
                "state": state,
                "oos": data["oos"],
                "all": data["all"]
            }
            for state, data in top_states
        ],
        "biggest_movers": biggest_movers,
        "state_count": len([s for s in state_data.values() if s["oos"] > 0])
    }
    
    print(f"✓ Processed {total_all} total violations ({total_oos} OOS)")
    print(f"✓ {len(state_data)} states affected")
    print(f"✓ {len(sorted_months)} months of data")
    
    return result

def calculate_biggest_movers(state_monthly, sorted_months):
    """
    Calculate states with biggest month-over-month changes
    
    Args:
        state_monthly: State-level monthly data
        sorted_months: List of months in order
    
    Returns:
        Dictionary with top increases and decreases
    """
    if len(sorted_months) < 2:
        return {"increases": [], "decreases": []}
    
    current_month = sorted_months[-1]
    previous_month = sorted_months[-2]
    
    changes = []
    
    for state, monthly_data in state_monthly.items():
        current = monthly_data[current_month]["oos"]
        previous = monthly_data[previous_month]["oos"]
        
        if previous > 0:  # Only calculate if there was previous data
            pct_change = ((current - previous) / previous) * 100
            changes.append({
                "state": state,
                "previous": previous,
                "current": current,
                "change": round(pct_change, 1)
            })
    
    # Sort by change
    changes.sort(key=lambda x: x["change"], reverse=True)
    
    # Get top 3 increases and decreases
    increases = [c for c in changes if c["change"] > 0][:3]
    decreases = [c for c in changes if c["change"] < 0][:3]
    
    return {
        "increases": increases,
        "decreases": decreases
    }

def save_data(data, filename="elp_data.json"):
    """
    Save processed data to JSON file
    
    Args:
        data: Processed violation data
        filename: Output filename
    """
    print(f"\nSaving data to {filename}...")
    
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✓ Data saved successfully")
        return True
    except Exception as e:
        print(f"✗ Error saving data: {e}")
        return False

def main():
    """Main execution function"""
    print("=" * 60)
    print("FMCSA ELP Violation Data Fetcher")
    print("Trucksafe Consulting, LLC")
    print("=" * 60)
    print()
    
    # Fetch all violations
    violations = fetch_all_elp_violations()
    
    if not violations:
        print("\n✗ No violations found. This might be due to:")
        print("  - API connectivity issues")
        print("  - No ELP violations in the dataset yet")
        print("  - Incorrect violation code")
        sys.exit(1)
    
    # Process violations
    processed_data = process_violations(violations)
    
    # Save to file
    if save_data(processed_data):
        print("\n" + "=" * 60)
        print("✓ SUCCESS - Data pipeline completed")
        print("=" * 60)
        print(f"\nSummary:")
        print(f"  • Total OOS violations: {processed_data['total_oos']}")
        print(f"  • OOS Rate: {processed_data['oos_rate']}%")
        print(f"  • States affected: {processed_data['state_count']}")
        print(f"  • Months of data: {len(processed_data['monthly']['labels'])}")
        print(f"  • Last updated: {processed_data['last_updated']}")
        print()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
