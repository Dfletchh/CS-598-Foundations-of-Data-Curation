"""
Combine all election year CSV files into one master dataset
"""

import pandas as pd
from pathlib import Path
import sys

# Combine all election year files into one master dataset
def combine_election_data():
    current_dir = Path.cwd()
    print(f"\nCurrent directory: {current_dir}")
    
    if current_dir.name == "scripts":
        BASE_DIR = current_dir.parent
    else:
        BASE_DIR = current_dir
    
    # Set paths
    INPUT_DIR = BASE_DIR / "data" / "raw" / "elections" / "election_results"
    OUTPUT_DIR = BASE_DIR / "data" / "processed"
    
    # Check if input directory exists
    if not INPUT_DIR.exists():
        input("\nWrong IO Directory...")
        return
    
    # Files to combine
    years = [2016, 2018, 2020, 2022, 2024]
    dataframes = []
    
    for year in years:
        file_path = INPUT_DIR / f"Voter_Turnout_{year}.csv"
        
        if file_path.exists():
            try:
                df = pd.read_csv(file_path)
                dataframes.append(df)
            except Exception as e:
                print(f"\tError reading file: {e}")
    
    if not dataframes:
        print("\nData files were successfully loaded")
        return
    
    # Combine all years
    try:
        combined = pd.concat(dataframes, ignore_index=True)
        
        if len(combined) == len(years) * 67:
            print("Record count matches expected")
        else:
            print(f"Issue: Expected {len(years) * 67} records, got {len(combined)}")
        
    except Exception as e:
        print(f"\nError combining data: {e}")
        return
    
    combined = combined.sort_values(['County', 'Year']).reset_index(drop=True)
    
    try:
        summary = combined.groupby('Year').agg({
            'Turnout_Percent': ['mean', 'min', 'max'],
            'Registered_Voters': 'sum',
            'Votes_Cast': 'sum'
        }).round(2)
        
        summary.columns = ['Avg_Turnout_%', 'Min_Turnout_%', 'Max_Turnout_%', 
                          'Total_Registered', 'Total_Votes']
        print(summary)
    except Exception as e:
        print(f"Could not calculate summary: {e}")
    
    # Create output directory
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        print(f"\nOutput directory ready: {OUTPUT_DIR}")
    except Exception as e:
        print(f"\nError: {e}")
        return
    
    # Save combined file
    output_file = OUTPUT_DIR / "All_Elections_Combined_2016_2024.csv"
    
    try:
        combined.to_csv(output_file, index=False)
        
        # Show preview
        print("\nPreview:")
        print(combined.head(10).to_string())
        
    except Exception as e:
        print(f"\nError: {e}")
        return

if __name__ == "__main__":
    try:
        combine_election_data()
    except Exception as e:
        print(f"\n\nUNEXPECTED ERROR: {e}")
        print("\nTrace:")
        import traceback
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")