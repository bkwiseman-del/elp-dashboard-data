#!/usr/bin/env python3
"""
FMCSA ELP Violation Data Fetcher - Corrected Field Names
Fetches English Language Proficiency violation data from FMCSA via Socrata API
Cross-references Violations and Inspections datasets to get state-level data
"""

import requests
import json
from datetime import datetime
from collections import defaultdict
import sys
import time

# Socrata API endpoints
VIOLATIONS_API = "https://data.transportation.gov/resource/8mt8-2mdr.json"
INSPECTIONS_API = "https://data.transportation.gov/resource/rbkj-cgst.json"

# Date when OOS criteria was restored
OOS_RESTORATION_DATE = "2025-06-25"

def fetch_elp_violations(limit=10000, offset=0):
    """
    Fetch ELP violations from FMCSA Violations dataset
    Uses correct field names: insp_date, oos_indicator, basic_desc, section_desc
    """
    print(f"Fetching violations from FMCSA (offset: {offset})...")
    
    # Query using correct field name: insp_date (not inspection_date)
    params = {
        "$where": f"insp_date >= '{OOS_RESTORATION_DATE}T00:00:00' AND basic_desc = 'Driver Fitness'",
        "$limit": limit,
        "$offset": offset,
        "$order": "insp_date DESC",
        "$select": "unique_id, insp_date, oos_indicator, section_desc, viol_code, basic_desc"
    }
    
    try:
        response = requests.get(VIOLATIONS_API, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Filter for ELP violations in Python
        elp_violations = []
        for record in data:
            section_desc = str(record.get("section_desc", "")).lower()
            viol_code = str(record.get("viol_code", "")).lower()
            
            # Check if this is an ELP violation
            if "english" in section_desc or "391.11" in viol_code or "391.11" in section_desc:
                elp_violations.append(record)
        
        print(f"✓ Fetched {len(data)} Driver Fitness violations, {len(elp_violations)} are ELP")
        return elp_violations
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching violations: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response: {e.response.text[:500]}")
        return []

def fetch_inspection_states(unique_ids):
    """
    Fetch state information (report_state) for given inspection IDs
    Strategy: Fetch all Driver Fitness inspections since June 2025 and match in Python
    This avoids 414 Request-URI Too Large errors from complex queries
    """
    if not unique_ids:
        return {}
    
    print(f"Fetching inspection state data for {len(unique_ids)} violations...")
    print("Strategy: Fetching all recent Driver Fitness inspections and matching in Python...")
    
    # Convert list to set for fast lookup
    target_ids = set(unique_ids)
    state_map = {}
    
    # Fetch inspections in batches
    limit = 10000
    offset = 0
    max_batches = 5
    
    for batch_num in range(max_batches):
        print(f"  Fetching inspection batch {batch_num + 1}...")
        
        params = {
            "$where": f"insp_date >= '{OOS_RESTORATION_DATE}T00:00:00' AND dr_fitness_insp = 'Y'",
            "$select": "unique_id, report_state",
            "$limit": limit,
            "$offset": offset,
            "$order": "insp_date DESC"
        }
        
        try:
            response = requests.get(INSPECTIONS_API, params=params, timeout=60)
            response.raise_for_status()
            inspections = response.json()
            
            if not inspections:
                print(f"  No more inspections found, stopping.")
                break
            
            # Match inspections to our violation IDs
            matches = 0
            for inspection in inspections:
                uid = inspection.get("unique_id")
                if uid in target_ids:
                    state = inspection.get("report_state")
                    if state:
                        state_map[uid] = state
                        matches += 1
            
            print(f"  ✓ Batch {batch_num + 1}: Found {matches} matching inspections (total mapped: {len(state_map)})")
            
            # If we've mapped all violations, we can stop
            if len(state_map) >= len(target_ids):
                print(f"  All violations mapped, stopping early.")
                break
            
            # If we got less than limit, we've reached the end
            if len(inspections) < limit:
                print(f"  Reached end of inspections.")
                break
            
            offset += limit
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error fetching inspection batch: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"     Status: {e.response.status_code}")
            continue
    
    print(f"✓ Successfully mapped {len(state_map)} of {len(unique_ids)} violations to states")
    return state_map

def fetch_all_elp_data():
    """
    Fetch all ELP violations and join with state data
    """
    all_violations = []
    offset = 0
    limit = 10000
    max_batches = 20  # Fetch up to 200k records to get all 10k+ ELP violations
    
    print("Starting to fetch Driver Fitness violations...")
    
    for batch_num in range(max_batches):
        print(f"\n--- Batch {batch_num + 1} ---")
        batch = fetch_elp_violations(limit=limit, offset=offset)
        
        if not batch:
            print("No more violations found.")
            break
        
        all_violations.extend(batch)
        
        # If we got less than limit, we've reached the end
        if len(batch) < limit:
            print(f"Received {len(batch)} violations (less than limit), stopping.")
            break
        
        offset += limit
        print(f"Total ELP violations so far: {len(all_violations)}")
        
        # Continue until we stop finding ELP violations
    
    print(f"\n✓ Total ELP violations fetched: {len(all_violations)}")
    
    if not all_violations:
        return []
    
    # Extract unique inspection IDs
    unique_ids = list(set([v.get("unique_id") for v in all_violations if v.get("unique_id")]))
    print(f"Unique inspection IDs: {len(unique_ids)}")
    
    # Fetch state data for these inspections
    state_map = fetch_inspection_states(unique_ids)
    
    # Join violations with state data
    violations_with_states = []
    for violation in all_violations:
        uid = violation.get("unique_id")
        if uid and uid in state_map:
            violation["state"] = state_map[uid]
            violations_with_states.append(violation)
    
    print(f"✓ Violations with state data: {len(violations_with_states)}")
    
    return violations_with_states

def process_violations(violations):
    """Process and aggregate violation data"""
    print("\nProcessing violations...")
    
    monthly_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_monthly = defaultdict(lambda: defaultdict(lambda: {"oos": 0, "all": 0}))
    
    total_oos = 0
    total_all = 0
    
    for violation in violations:
        try:
            # Get insp_date (correct field name)
            date_str = violation.get("insp_date")
            if not date_str:
                continue
            
            # Extract year-month
            date_obj = datetime.fromisoformat(str(date_str).split("T")[0])
            year_month = date_obj.strftime("%Y-%m")
            
            # Get state (from our join)
            state = violation.get("state", "UNKNOWN")
            if not state or state == "UNKNOWN":
                continue
            
            # Check if OOS using oos_indicator field
            oos_indicator = str(violation.get("oos_indicator", "")).upper()
            is_oos = oos_indicator in ["Y", "YES"]
            
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
    
    # Prepare monthly arrays
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
    
    # Calculate biggest movers
    biggest_movers = calculate_biggest_movers(state_monthly, sorted_months)
    
    # Calculate statistics
    avg_per_month = total_oos / len(sorted_months) if sorted_months else 0
    peak_month = max(monthly_data.items(), key=lambda x: x[1]["oos"]) if monthly_data else (None, {"oos": 0})
    peak_month_label = datetime.strptime(peak_month[0], "%Y-%m").strftime("%b '%y") if peak_month[0] else "N/A"
    
    # Month-over-month percentage
    if len(monthly_oos) >= 2:
        current_month = monthly_oos[-1]
        last_month = monthly_oos[-2]
        mom_change = ((current_month - last_month) / last_month * 100) if last_month > 0 else 0
    else:
        mom_change = 0
    
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
        "state_count": len([s for s in state_data.values() if s["oos"] > 0]),
        "data_source": "real"
    }
    
    print(f"✓ Processed {total_all} total violations ({total_oos} OOS)")
    print(f"✓ {len(state_data)} states affected")
    print(f"✓ {len(sorted_months)} months of data")
    
    return result

def calculate_biggest_movers(state_monthly, sorted_months):
    """Calculate states with biggest month-over-month changes"""
    if len(sorted_months) < 2:
        return {"increases": [], "decreases": []}
    
    current_month = sorted_months[-1]
    previous_month = sorted_months[-2]
    
    changes = []
    
    for state, monthly_data in state_monthly.items():
        current = monthly_data[current_month]["oos"]
        previous = monthly_data[previous_month]["oos"]
        
        if previous > 0:
            pct_change = ((current - previous) / previous) * 100
            changes.append({
                "state": state,
                "previous": previous,
                "current": current,
                "change": round(pct_change, 1)
            })
    
    changes.sort(key=lambda x: x["change"], reverse=True)
    
    increases = [c for c in changes if c["change"] > 0][:3]
    decreases = [c for c in changes if c["change"] < 0][:3]
    
    return {"increases": increases, "decreases": decreases}

def save_data(data, filename="elp_data.json"):
    """Save processed data to JSON file"""
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
    
    # Fetch violations with state data
    violations = fetch_all_elp_data()
    
    if not violations or len(violations) == 0:
        print("\n⚠ No ELP violations found with state data.")
        print("This might be due to:")
        print("  - API connectivity issues")
        print("  - Data not yet available")
        print("  - Query parameters need adjustment")
        sys.exit(1)
    
    # Process violations
    processed_data = process_violations(violations)
    
    # Save to file
    if save_data(processed_data):
        print("\n" + "=" * 60)
        print("✓ SUCCESS - Real FMCSA data processed")
        print("=" * 60)
        print(f"\nSummary:")
        print(f"  • Total OOS violations: {processed_data['total_oos']:,}")
        print(f"  • Total all violations: {processed_data['total_all']:,}")
        print(f"  • OOS Rate: {processed_data['oos_rate']}%")
        print(f"  • States affected: {processed_data['state_count']}")
        print(f"  • Months of data: {len(processed_data['monthly']['labels'])}")
        print(f"  • Last updated: {processed_data['last_updated']}")
        print(f"  • Data source: REAL FMCSA DATA")
        print()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
