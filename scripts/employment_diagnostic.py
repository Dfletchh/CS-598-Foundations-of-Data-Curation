"""
Employment Variable Diagnostic
Find the correct description/line code for total employment in CAINC4
"""

import pandas as pd
from pathlib import Path

current_dir = Path.cwd()
if current_dir.name == "scripts":
    BASE_DIR = current_dir.parent
else:
    BASE_DIR = current_dir

BEA_DIR = BASE_DIR / "data" / "raw" / "bea"

print("=" * 70)
print("CAINC4 EMPLOYMENT DIAGNOSTIC")
print("=" * 70)

emp_file = BEA_DIR / "employment" / "CAINC4_FL_1969_2023.csv"
emp = pd.read_csv(emp_file, encoding='latin1')

print(f"\nTotal rows in file: {len(emp)}")

# Clean GeoFIPS
emp['GeoFIPS_clean'] = emp['GeoFIPS'].str.strip().str.strip('"').str.strip()

# Filter for counties (not state total)
emp_counties = emp[emp['GeoFIPS_clean'].str.len() == 5]
emp_counties = emp_counties[emp_counties['GeoFIPS_clean'].str.startswith('12')]
emp_counties = emp_counties[emp_counties['GeoFIPS_clean'] != '12000']

print(f"County-level rows: {len(emp_counties)}")

# Show unique descriptions
print("\n" + "=" * 70)
print("ALL UNIQUE DESCRIPTIONS IN COUNTY DATA:")
print("=" * 70)
emp_counties['Description_clean'] = emp_counties['Description'].str.strip()
unique_desc = emp_counties['Description_clean'].unique()

for i, desc in enumerate(unique_desc, 1):
    print(f"{i:2d}. {desc}")

# Show unique line codes
print("\n" + "=" * 70)
print("ALL UNIQUE LINE CODES:")
print("=" * 70)
unique_codes = emp_counties['LineCode'].unique()
print(sorted(unique_codes))

# Search for employment-related descriptions
print("\n" + "=" * 70)
print("EMPLOYMENT-RELATED DESCRIPTIONS:")
print("=" * 70)
employment_keywords = ['employ', 'job', 'work', 'labor', 'labour']
for keyword in employment_keywords:
    matches = emp_counties[emp_counties['Description_clean'].str.contains(keyword, case=False, na=False)]
    if len(matches) > 0:
        print(f"\n'{keyword}' matches ({len(matches)} rows):")
        for desc in matches['Description_clean'].unique():
            matching_codes = matches[matches['Description_clean'] == desc]['LineCode'].unique()
            print(f"  LineCode {matching_codes[0]}: {desc}")

# Check one county in detail
print("\n" + "=" * 70)
print("DONE!")
print("=" * 70)
alachua = emp_counties[emp_counties['GeoFIPS_clean'] == '12001']
print(alachua[['LineCode', 'Description_clean', '2023']].to_string(index=False))

print("\n" + "=" * 70)
input("\nPress Enter to exit...")
