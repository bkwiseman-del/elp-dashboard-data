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

# Date when we start counting ELP violations (all of 2025 forward)
OOS_RESTORATION_DATE = "2025-01-01"

def fetch_elp_violations(limit=10000, offset=0):
    """
    Fetch ELP violations from FMCSA Violations dataset
    Returns: (elp_violations_list, total_fetched_count)
    """
    print(f"Fetching Driver Fitness violations from FMCSA (offset: {offset})...")
    
    # Fetch ALL Driver Fitness violations (not just ELP)
    params = {
        "$where": f"Insp_Date >= '{OOS_RESTORATION_DATE}T00:00:00' AND BASIC_Desc = 'Driver Fitness'",
        "$limit": limit,
        "$offset": offset,
        "$order": "Insp_Date DESC",
        "$select": "Unique_ID, Insp_Date, OOS_Indicator, Section_Desc, Viol_Code, BASIC_Desc"
    }
    
    try:
        response = requests.get(VIOLATIONS_API, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        total_fetched = len(data)
        
        # Normalize field names
        normalized_data = []
        for record in data:
            normalized = {
                "unique_id": record.get("Unique_ID") or record.get("unique_id"),
                "insp_date": record.get("Insp_Date") or record.get("insp_date"),
                "oos_indicator": record.get("OOS_Indicator") or record.get("oos_indicator"),
                "section_desc": record.get("Section_Desc") or record.get("section_desc"),
                "viol_code": record.get("Viol_Code") or record.get("viol_code"),
                "basic_desc": record.get("BASIC_Desc") or record.get("basic_desc")
            }
            normalized_data.append(normalized)
        
        # Filter for ELP violations - ONLY use "English" in Section_Desc
        # This is the most reliable way per user's manual analysis
        elp_violations = []
        for record in normalized_data:
            section_desc = str(record.get("section_desc", "")).lower()
            
            # Check if this is an ELP violation - look for "english" only
            if "english" in section_desc:
                elp_violations.append(record)
        
        print(f"✓ Fetched {total_fetched} Driver Fitness violations, {len(elp_violations)} are ELP")
        
        # Return BOTH the ELP violations AND the total count
        return elp_violations, total_fetched
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching violations: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response: {e.response.text[:500]}")
        return [], 0

def fetch_inspection_states(unique_ids):
    """
    Fetch state information (Report_State) for given inspection IDs
    Strategy: Fetch all inspections since June 2025 and match in Python
    This avoids 414 Request-URI Too Large errors from complex queries
    """
    if not unique_ids:
        return {}
    
    print(f"Fetching inspection state data for {len(unique_ids)} violations...")
    print("Strategy: Fetching all recent inspections and matching in Python...")
    
    # Convert list to set for fast lookup
    target_ids = set(unique_ids)
    state_map = {}
    
    # Fetch inspections in batches - NO FILTER, just get all recent ones
    limit = 10000
    offset = 0
    max_batches = 30  # Reduced to 30 to avoid rate limits and finish faster
    
    for batch_num in range(max_batches):
        print(f"  Fetching inspection batch {batch_num + 1}...")
        
        # Use capitalized field names from documentation
        params = {
            "$where": f"Insp_Date >= '{OOS_RESTORATION_DATE}T00:00:00'",
            "$select": "Unique_ID, Report_State",
            "$limit": limit,
            "$offset": offset,
            "$order": "Insp_Date DESC"
        }
        
        try:
            response = requests.get(INSPECTIONS_API, params=params, timeout=60)
            response.raise_for_status()
            inspections = response.json()
            
            if not inspections:
                print(f"  No more inspections found, stopping.")
                break
            
            # Match inspections to our violation IDs (handle both cases)
            matches = 0
            for inspection in inspections:
                uid = inspection.get("Unique_ID") or inspection.get("unique_id")
                if uid in target_ids:
                    state = inspection.get("Report_State") or inspection.get("report_state")
                    if state:
                        state_map[uid] = state
                        matches += 1
            
            print(f"  ✓ Batch {batch_num + 1}: Found {matches} matching inspections (total mapped: {len(state_map)})")
            
            # If we've mapped most violations, we can stop
            if len(state_map) >= len(target_ids) * 0.95:
                print(f"  Mapped 95%+ of violations, stopping early.")
                break
            
            # If we got less than limit, we've reached the end
            if len(inspections) < limit:
                print(f"  Reached end of inspections.")
                break
            
            offset += limit
            
            # CRITICAL: Add delay to avoid API rate limiting
            time.sleep(2)
            
            # CRITICAL: Add delay to avoid API rate limiting
            # FMCSA has strict rate limits - wait 2 seconds between batches
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error fetching inspection batch: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"     Status: {e.response.status_code}")
            continue
    
    print(f"✓ Successfully mapped {len(state_map)} of {len(unique_ids)} violations to states ({len(state_map)/len(unique_ids)*100:.1f}%)")
    return state_map

def fetch_all_elp_data():
    """
    Fetch all ELP violations and join with state data
    """
    all_violations = []
    offset = 0
    limit = 10000
    max_batches = 25  # Reduced to 25 to avoid API rate limits (will get ~250k DF violations, ~37k ELP)
    
    print("Starting to fetch Driver Fitness violations...")
    
    for batch_num in range(max_batches):
        print(f"\n--- Batch {batch_num + 1} ---")
        
        # Get both ELP violations and total count fetched
        elp_batch, total_fetched = fetch_elp_violations(limit=limit, offset=offset)
        
        if not elp_batch and total_fetched == 0:
            print("No more violations found.")
            break
        
        all_violations.extend(elp_batch)
        
        # IMPORTANT: Check if we fetched LESS Driver Fitness violations than the limit
        # This means we've reached the end of the dataset
        if total_fetched < limit:
            print(f"Fetched {total_fetched} Driver Fitness violations (less than limit of {limit}), reached end of dataset.")
            break
        
        offset += limit
        print(f"Total ELP violations so far: {len(all_violations)}")
    
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
            
            # Parse FMCSA date format: DD-MMM-YY (e.g., "26-DEC-23" or "31-OCT-25")
            try:
                # Try parsing DD-MMM-YY format
                date_obj = datetime.strptime(str(date_str).strip().upper(), "%d-%b-%y")
            except:
                try:
                    # Fallback: try ISO format
                    date_obj = datetime.fromisoformat(str(date_str).split("T")[0])
                except:
                    # Can't parse, skip this violation
                    continue
            
            year_month = date_obj.strftime("%Y-%m")
            
            # Get state (from our join)
            state = violation.get("state", "UNKNOWN")
            if not state or state == "UNKNOWN":
                continue
            
            # Check if OOS - FMCSA uses "TRUE"/"FALSE" format
            oos_indicator = str(violation.get("oos_indicator", "")).upper().strip()
            oos_indicator_alt = str(violation.get("OOS_Indicator", "")).upper().strip()
            
            # Check for "TRUE", "T", "Y", "YES", "1"
            is_oos = (oos_indicator in ["TRUE", "T", "Y", "YES", "1"] or 
                     oos_indicator_alt in ["TRUE", "T", "Y", "YES", "1"])
            
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
