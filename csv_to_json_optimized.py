#!/usr/bin/env python3
"""
FMCSA ELP Data CSV Converter - OPTIMIZED VERSION
Converts the new Vehicle Inspection File and Vehicle Inspections datasets
Uses better datasets with built-in state data

REQUIRED CSVs:
1. Vehicle Inspection File (fx4q-ay7w) - has state data built-in
2. Vehicle Inspections and Violations (876r-jsdb) - has violation details

USAGE:
1. Download both CSVs from data.transportation.gov
2. Rename them to:
   - inspections.csv (Vehicle Inspection File)
   - violations.csv (Vehicle Inspections and Violations)
3. Place both files in the same folder as this script
4. Run: python3 csv_to_json_optimized.py
5. Upload the generated elp_data.json to your GitHub repo
"""

import csv
import json
from datetime import datetime
from collections import defaultdict
import sys

# File names
INSPECTIONS_FILE = "inspections.csv"
VIOLATIONS_FILE = "violations.csv"
OUTPUT_FILE = "elp_data.json"

def load_elp_violations(filename):
    """
    Load violations CSV and find all ELP violation inspection IDs
    Filters for 391.11(b)(2) specifically - all variations
    ONLY for 2025 and later to match what's in inspections CSV
    Returns dict: {inspection_id: is_elp_oos}
    """
    print(f"Loading violations from {filename}...")
    print("Filtering for ELP violations (391.11(b)(2)) from 2025+...")
    
    elp_violations = {}  # Changed from set to dict to store OOS status
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            elp_count = 0
            
            for row in reader:
                count += 1
                
                # Get part number and section
                # These are the actual field names in the dataset
                part_no = (row.get('PART_NO') or row.get('part_no') or '').strip()
                part_section = (row.get('PART_NO_SECTION') or row.get('part_no_section') or '').strip().upper()
                
                # Check if this is an ELP violation (Part 391, Section 11(b)(2))
                # Catches ALL variations: 11(b)(2), 11B2, 11B2-S, 11B2-Q, 11B2-Z, etc.
                is_elp = (
                    part_no == '391' and (
                        part_section == '11(B)(2)' or
                        part_section.startswith('11B2')  # Catches 11B2, 11B2-Z, 11B2-Q, 11B2-S, etc.
                    )
                )
                
                if is_elp:
                    # Check date - only include 2025+
                    # This dataset uses CHANGE_DATE, not INSP_DATE
                    change_date = (row.get('CHANGE_DATE') or row.get('change_date') or '').strip()
                    
                    # Extract first 8 characters (YYYYMMDD) before the space
                    date_part = change_date.split()[0] if change_date else ''
                    
                    if len(date_part) == 8 and date_part.isdigit():
                        year = int(date_part[:4])
                        if year >= 2025:
                            inspection_id = row.get('INSPECTION_ID') or row.get('inspection_id')
                            
                            # Check if THIS ELP VIOLATION was OOS
                            oos_indicator = (row.get('OUT_OF_SERVICE_INDICATOR') or row.get('out_of_service_indicator') or '').strip().upper()
                            is_elp_oos = oos_indicator in ['TRUE', 'T', 'Y', 'YES', '1']
                            
                            if inspection_id:
                                # Store OOS status for this specific ELP violation
                                elp_violations[inspection_id] = is_elp_oos
                                elp_count += 1
                
                if count % 100000 == 0:
                    print(f"  Processed {count:,} violations... Found {elp_count:,} ELP (2025+) so far")
        
        print(f"✓ Processed {count:,} total violations")
        print(f"✓ Found {len(elp_violations):,} inspections with ELP violations")
        return elp_violations
        
    except FileNotFoundError:
        print(f"✗ Error: Could not find {filename}")
        print(f"  Make sure you've downloaded the Vehicle Inspections and Violations CSV")
        print(f"  and renamed it to {filename}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading {filename}: {e}")
        sys.exit(1)

def process_inspections(filename, elp_inspection_ids):
    """
    Process inspections CSV - only those with ELP violations
    This dataset has state data built-in!
    """
    print(f"\nLoading inspections from {filename}...")
    
    monthly_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_data = defaultdict(lambda: {"oos": 0, "all": 0})
    state_monthly = defaultdict(lambda: defaultdict(lambda: {"oos": 0, "all": 0}))
    
    total_oos = 0
    total_all = 0
    matched = 0
    skipped = 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            
            for row in reader:
                count += 1
                
                try:
                    # Check if this inspection is an ELP violation
                    inspection_id = row.get('INSPECTION_ID') or row.get('inspection_id')
                    if inspection_id not in elp_inspection_ids:
                        continue
                    
                    matched += 1
                    
                    # Get state (built-in to this dataset!)
                    state = row.get('REPORT_STATE') or row.get('report_state')
                    if not state:
                        skipped += 1
                        continue
                    
                    # Parse date - handle multiple formats
                    date_str = row.get('INSP_DATE') or row.get('insp_date')
                    if not date_str:
                        skipped += 1
                        continue
                    
                    # Try different date formats
                    date_obj = None
                    date_str = str(date_str).strip()
                    
                    # Try YYYYMMDD format first (most common in this dataset)
                    if len(date_str) == 8 and date_str.isdigit():
                        try:
                            date_obj = datetime.strptime(date_str, "%Y%m%d")
                        except:
                            pass
                    
                    # Try ISO format (YYYY-MM-DD)
                    if not date_obj:
                        try:
                            date_obj = datetime.fromisoformat(date_str.split("T")[0])
                        except:
                            pass
                    
                    # Try DD-MMM-YY format (e.g., "26-DEC-23")
                    if not date_obj:
                        try:
                            date_obj = datetime.strptime(date_str.upper(), "%d-%b-%y")
                        except:
                            pass
                    
                    # Try MM/DD/YYYY format
                    if not date_obj:
                        try:
                            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                        except:
                            pass
                    
                    if not date_obj:
                        skipped += 1
                        continue
                    
                    # Only include 2025 and later
                    if date_obj.year < 2025:
                        continue
                    
                    year_month = date_obj.strftime("%Y-%m")
                    
                    # Check if THIS ELP VIOLATION was OOS (from violations CSV)
                    is_elp_oos = elp_inspection_ids[inspection_id]
                    
                    # Increment counters
                    monthly_data[year_month]["all"] += 1
                    total_all += 1
                    state_data[state]["all"] += 1
                    state_monthly[state][year_month]["all"] += 1
                    
                    if is_elp_oos:
                        monthly_data[year_month]["oos"] += 1
                        total_oos += 1
                        state_data[state]["oos"] += 1
                        state_monthly[state][year_month]["oos"] += 1
                    
                    if matched % 5000 == 0:
                        print(f"  Processed {matched:,} ELP inspections...")
                
                except Exception as e:
                    skipped += 1
                    continue
                
                if count % 100000 == 0:
                    print(f"  Scanned {count:,} inspections...")
        
        print(f"✓ Scanned {count:,} total inspections")
        print(f"✓ Matched {matched:,} ELP inspections")
        print(f"  • {total_oos:,} OOS violations")
        print(f"  • {total_all:,} total violations")
        if skipped > 0:
            print(f"  ⚠ Skipped {skipped:,} due to missing data")
        
    except FileNotFoundError:
        print(f"✗ Error: Could not find {filename}")
        print(f"  Make sure you've downloaded the Vehicle Inspection File CSV")
        print(f"  and renamed it to {filename}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading {filename}: {e}")
        sys.exit(1)
    
    return monthly_data, state_data, state_monthly, total_oos, total_all

def calculate_biggest_movers(state_monthly, sorted_months):
    """Calculate states with biggest month-over-month changes
    
    Uses last 2 FULL months to avoid incomplete current month data
    Filters out low-volume states to avoid skewed percentages
    (e.g., going from 1 to 0 = -100% but not meaningful)
    """
    if len(sorted_months) < 3:
        return {"increases": [], "decreases": []}
    
    # Use last 2 FULL months (skip the most recent which may be incomplete)
    current_month = sorted_months[-2]  # Last FULL month
    previous_month = sorted_months[-3]  # Previous FULL month
    
    changes = []
    for state, months in state_monthly.items():
        current = months.get(current_month, {}).get("oos", 0)
        previous = months.get(previous_month, {}).get("oos", 0)
        
        # FILTER: Only include states with at least 5 violations in previous month
        # This avoids misleading percentages from low-volume states
        if previous >= 5:
            pct_change = ((current - previous) / previous) * 100
            changes.append({
                "state": state,
                "change": round(pct_change, 1),
                "current": current,
                "previous": previous
            })
    
    changes.sort(key=lambda x: x["change"], reverse=True)
    
    return {
        "increases": changes[:3] if len(changes) >= 3 else changes,
        "decreases": changes[-3:][::-1] if len(changes) >= 3 else []
    }

def generate_json(monthly_data, state_data, state_monthly, total_oos, total_all):
    """Generate final JSON output"""
    print("\nGenerating JSON...")
    
    # Sort months
    sorted_months = sorted(monthly_data.keys())
    
    # Monthly arrays
    monthly_labels = []
    monthly_oos = []
    monthly_all = []
    
    for month in sorted_months:
        date_obj = datetime.strptime(month, "%Y-%m")
        label = date_obj.strftime("%b %y")
        monthly_labels.append(label)
        monthly_oos.append(monthly_data[month]["oos"])
        monthly_all.append(monthly_data[month]["all"])
    
    # Top 10 states
    top_states = sorted(state_data.items(), key=lambda x: x[1]["oos"], reverse=True)[:10]
    
    # Calculate statistics
    avg_per_month = total_oos / len(sorted_months) if sorted_months else 0
    peak_month = max(monthly_data.items(), key=lambda x: x[1]["oos"]) if monthly_data else (None, {"oos": 0})
    peak_month_label = datetime.strptime(peak_month[0], "%Y-%m").strftime("%b '%y") if peak_month[0] else "N/A"
    
    # Month-over-month change - use last 2 FULL months (exclude current incomplete month)
    if len(monthly_oos) >= 3:
        # Use [-2] and [-3] to avoid incomplete current month
        last_full_month = monthly_oos[-2]
        previous_full_month = monthly_oos[-3]
        mom_change = ((last_full_month - previous_full_month) / previous_full_month * 100) if previous_full_month > 0 else 0
    elif len(monthly_oos) >= 2:
        # Fallback if we only have 2 months
        current_month = monthly_oos[-1]
        last_month = monthly_oos[-2]
        mom_change = ((current_month - last_month) / last_month * 100) if last_month > 0 else 0
    else:
        mom_change = 0
    
    # Biggest movers
    biggest_movers = calculate_biggest_movers(state_monthly, sorted_months)
    
    # Build result
    result = {
        "last_updated": datetime.now().strftime("%B %d, %Y"),
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
        "state_monthly": {
            state: {
                month_label: {
                    "oos": months[month_key]["oos"],
                    "all": months[month_key]["all"]
                }
                for month_key, month_label in zip(sorted_months, monthly_labels)
                if month_key in months
            }
            for state, months in state_monthly.items()
        },
        "biggest_movers": biggest_movers,
        "state_count": len([s for s in state_data.values() if s["oos"] > 0]),
        "data_source": "real"
    }
    
    return result

def main():
    print("=" * 70)
    print("FMCSA ELP Data Converter - OPTIMIZED VERSION")
    print("Using Vehicle Inspection File + Vehicle Inspections and Violations")
    print("=" * 70)
    print()
    
    # Step 1: Load violations and find ELP inspection IDs
    elp_inspection_ids = load_elp_violations(VIOLATIONS_FILE)
    
    if not elp_inspection_ids:
        print("\n✗ No ELP violations found. Check your violations CSV.")
        sys.exit(1)
    
    # Step 2: Process inspections (with state data built-in!)
    monthly_data, state_data, state_monthly, total_oos, total_all = process_inspections(
        INSPECTIONS_FILE, 
        elp_inspection_ids
    )
    
    if total_all == 0:
        print("\n✗ No inspections matched. Check your data.")
        sys.exit(1)
    
    # Validate
    if total_all < 40000:
        print(f"\n⚠ Warning: Only {total_all:,} violations found, expected ~45,000")
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    # Generate JSON
    result = generate_json(monthly_data, state_data, state_monthly, total_oos, total_all)
    
    # Save
    print(f"\nSaving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(result, f, indent=2)
    
    print("\n" + "=" * 70)
    print("✓ SUCCESS!")
    print("=" * 70)
    print(f"\nGenerated {OUTPUT_FILE} with:")
    print(f"  • {result['total_oos']:,} OOS violations")
    print(f"  • {result['total_all']:,} total violations")
    print(f"  • {result['state_count']} states")
    print(f"  • {result['oos_rate']}% OOS rate")
    print(f"  • {len(result['monthly']['labels'])} months of data")
    print(f"\nNext steps:")
    print(f"  1. Upload {OUTPUT_FILE} to your GitHub repository")
    print(f"  2. Commit and push")
    print(f"  3. Your dashboard will update automatically!")
    print(f"\nDashboard: https://bkwiseman-del.github.io/elp-dashboard-data/")
    print("=" * 70)

if __name__ == "__main__":
    main()
