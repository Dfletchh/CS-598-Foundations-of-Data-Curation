"""
Cleans election data, adds FIPS codes, and performs quality checks
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

current_dir = Path.cwd()
if current_dir.name == "scripts":
    BASE_DIR = current_dir.parent
else:
    BASE_DIR = current_dir

print(f"Working from: {BASE_DIR}\n")

# Paths
ELECTION_FILE = BASE_DIR / "data" / "processed" / "All_Elections_Combined_2016_2024.csv"
FIPS_FILE = BASE_DIR / "data" / "raw" / "reference" / "florida_fips_codes.csv"
OUTPUT_DIR = BASE_DIR / "data" / "processed"
DOCS_DIR = BASE_DIR / "documentation"

# Load election data and FIPS reference
def load_data():
    # Load election data
    if not ELECTION_FILE.exists():
        print(f"Election file not found: {ELECTION_FILE}")
        return None, None
    
    elections = pd.read_csv(ELECTION_FILE)
    
    # Load FIPS reference
    if not FIPS_FILE.exists():
        print(f"FIPS file not found: {FIPS_FILE}")
        return elections, None
    
    fips = pd.read_csv(FIPS_FILE)
    
    return elections, fips

# Standardize county name variations
def standardize_county_names(df):
    df = df.copy()
    
    # Variations to standardize
    name_fixes = {
        'St. Johns': 'St. Johns',
        'Saint Johns': 'St. Johns',
        'St Johns': 'St. Johns',
        'St. Lucie': 'St. Lucie',
        'Saint Lucie': 'St. Lucie',
        'St Lucie': 'St. Lucie',
        'Miami-Dade': 'Miami-Dade',
        'Dade': 'Miami-Dade',
        'Miami Dade': 'Miami-Dade',
        'DeSoto': 'Desoto',
        'De Soto': 'Desoto'
    }
    
    # standardization
    df['County_Original'] = df['County']
    df['County'] = df['County'].str.strip()
    
    # Apply name fixes
    df['County'] = df['County'].replace(name_fixes)
    
    # Count changes
    changes = (df['County'] != df['County_Original']).sum()
    if changes > 0:
        print(f"\tStandardized {changes} county names")
        print(f"\tChanges made:")
        changed = df[df['County'] != df['County_Original']][['County_Original', 'County']].drop_duplicates()
        for _, row in changed.iterrows():
            print(f"\t\t{row['County_Original']} -> {row['County']}")
    
    return df

# Add FIPS codes to election data
def add_fips_codes(elections_df, fips_df):
    
    if fips_df is None:
        print("\tCannot add FIPS codes")
        return elections_df
    
    # Merge on county name
    merged = elections_df.merge(
        fips_df[['FIPS', 'County_Name']], 
        left_on='County', 
        right_on='County_Name',
        how='left'
    )
    
    # Check for unmatched counties
    unmatched = merged[merged['FIPS'].isna()]['County'].unique()
    
    if len(unmatched) > 0:
        print(f"\Notice: {len(unmatched)} counties not matched:")
        for county in unmatched:
            print(f"\t- {county}")
        print(f"  Total unmatched records: {merged['FIPS'].isna().sum()}")
    
    # Clean up
    merged = merged.drop('County_Name', axis=1)
    
    # Reorder columns
    cols = ['FIPS', 'County', 'Year', 'Election_Date', 'Registered_Voters', 
            'Votes_Cast', 'Turnout_Percent']
    if 'County_Original' in merged.columns:
        cols.insert(2, 'County_Original')
    
    merged = merged[cols]
    
    return merged

# Perform data quality checks
def quality_checks(df):
    issues = []
    
    # Check: Missing values
    missing = df.isnull().sum()
    if missing.any():
        print("  Checking for missing values:")
        for col, count in missing[missing > 0].items():
            print(f"\t- {col}: {count} missing values")
            issues.append(f"Missing values in {col}: {count}")
    
    # Check: Expected number of counties per year
    print("\n  Checking county counts per year:")
    counts = df.groupby('Year')['County'].nunique()
    for year, count in counts.items():
        status = "Pass" if count == 67 else "Fail"
        print(f"    {status} {year}: {count} counties (expected 67)")
        if count != 67:
            issues.append(f"Year {year} has {count} counties instead of 67")
    
    # Check: Turnout percentages in range
    print("\n  Checking turnout percentages:")
    low_turnout = df[df['Turnout_Percent'] < 30]
    high_turnout = df[df['Turnout_Percent'] > 95]
    
    if len(low_turnout) > 0:
        print(f"\t- {len(low_turnout)} records with turnout < 30%:")
        for _, row in low_turnout.iterrows():
            print(f"      {row['County']} {row['Year']}: {row['Turnout_Percent']:.1f}%")
            issues.append(f"Low turnout: {row['County']} {row['Year']} = {row['Turnout_Percent']:.1f}%")
    
    if len(high_turnout) > 0:
        print(f"\t- {len(high_turnout)} records with turnout > 95%:")
        for _, row in high_turnout.iterrows():
            print(f"\t\t{row['County']} {row['Year']}: {row['Turnout_Percent']:.1f}%")
    
    # Check: Verify turnout calculation
    print("\n  Verifying turnout calculations:")
    df['Calculated_Turnout'] = (df['Votes_Cast'] / df['Registered_Voters'] * 100).round(1)
    df['Turnout_Difference'] = (df['Calculated_Turnout'] - df['Turnout_Percent']).abs()
    
    mismatches = df[df['Turnout_Difference'] > 0.5]  # Allow 0.5% difference for rounding
    if len(mismatches) > 0:
        print(f"\t- {len(mismatches)} records with turnout calculation mismatches:")
        for _, row in mismatches.head(5).iterrows():
            print(f"\t\t{row['County']} {row['Year']}: Reported={row['Turnout_Percent']:.1f}%, Calculated={row['Calculated_Turnout']:.1f}%")
    
    # Clean up temporary columns
    df = df.drop(['Calculated_Turnout', 'Turnout_Difference'], axis=1)
    
    # Check: Duplicate records
    print("\n  Checking for duplicates:")
    duplicates = df.duplicated(subset=['County', 'Year'], keep=False)
    if duplicates.any():
        dup_count = duplicates.sum()
        print(f"\t\t- Found {dup_count} duplicate County-Year combinations:")
        dup_df = df[duplicates][['County', 'Year', 'Votes_Cast']].sort_values(['County', 'Year'])
        print(dup_df.head(10))
        issues.append(f"Duplicate records: {dup_count}")
    
    # Summary
    print(f"\n  {'='*66}")
    if len(issues) == 0:
        print("ALL QUALITY CHECKS PASSED")
    else:
        print(f"\t- Found {len(issues)} issues")
    print(f"  {'='*66}")
    
    return df, issues

# Create summary statistics
def create_summary_statistics(df):
    # Overall statistics
    print("\n  Dataset Overview:")
    print(f"\tTotal records: {len(df)}")
    print(f"\tYears covered: {df['Year'].min()} - {df['Year'].max()}")
    print(f"\tCounties: {df['County'].nunique()}")
    print(f"\tTotal votes (all years): {df['Votes_Cast'].sum():,.0f}")
    print(f"\tTotal registered (all years): {df['Registered_Voters'].sum():,.0f}")
    
    # Statistics by year
    print("\n  Turnout Statistics by Year:")
    year_stats = df.groupby('Year').agg({
        'Turnout_Percent': ['mean', 'std', 'min', 'max'],
        'Votes_Cast': 'sum',
        'Registered_Voters': 'sum'
    }).round(2)
    year_stats.columns = ['Avg_Turnout', 'Std_Dev', 'Min_Turnout', 'Max_Turnout', 
                          'Total_Votes', 'Total_Registered']
    print(year_stats.to_string())
    
    # Top and bottom turnout counties (2024)
    if 2024 in df['Year'].values:
        print("\n  2024 Election - Highest Turnout Counties:")
        top_2024 = df[df['Year'] == 2024].nlargest(5, 'Turnout_Percent')[
            ['County', 'Turnout_Percent', 'Votes_Cast', 'Registered_Voters']
        ]
        print(top_2024.to_string(index=False))
        
        print("\n  2024 Election - Lowest Turnout Counties:")
        bottom_2024 = df[df['Year'] == 2024].nsmallest(5, 'Turnout_Percent')[
            ['County', 'Turnout_Percent', 'Votes_Cast', 'Registered_Voters']
        ]
        print(bottom_2024.to_string(index=False))
    
    return year_stats

# Create data dictionary documentation
def create_data_dictionary(df):
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Define metadata for each column
    column_metadata = {
        'FIPS': {
            'description': 'Federal Information Processing Standard code for county (5 digits: 12XXX for Florida)',
            'source': 'U.S. Census Bureau / Florida Division of Elections',
            'notes': 'Used as primary key for joining with Census and BEA data'
        },
        'County': {
            'description': 'County name (standardized)',
            'source': 'Florida Division of Elections',
            'notes': 'Standardized to match Census Bureau naming conventions'
        },
        'County_Original': {
            'description': 'Original county name before standardization',
            'source': 'Florida Division of Elections',
            'notes': 'Kept for reference; may have minor variations in spelling'
        },
        'Year': {
            'description': 'Election year (2016, 2018, 2020, 2022, or 2024)',
            'source': 'Florida Division of Elections',
            'notes': 'Presidential elections (2016, 2020, 2024) have higher turnout than midterms (2018, 2022)'
        },
        'Election_Date': {
            'description': 'Date of general election (format: MM/DD/YYYY)',
            'source': 'Florida Division of Elections',
            'notes': 'Always first Tuesday after first Monday in November'
        },
        'Registered_Voters': {
            'description': 'Number of registered voters eligible to vote as of book closing (29 days before election)',
            'source': 'Florida Division of Elections',
            'notes': 'Book closing is 29 days before election; does not include election day registrations'
        },
        'Votes_Cast': {
            'description': 'Total number of votes cast in the general election',
            'source': 'Florida Division of Elections',
            'notes': 'Includes all voting methods: in-person, early voting, and mail-in ballots'
        },
        'Turnout_Percent': {
            'description': 'Percentage of registered voters who cast votes (Votes_Cast / Registered_Voters * 100)',
            'source': 'Calculated from Votes_Cast and Registered_Voters',
            'notes': 'Florida average ranges from 53.8% (2022 midterm) to 78.9% (2024 presidential)'
        }
    }
    
    # Build dictionary for actual columns in dataframe
    records = []
    for col in df.columns:
        if col in column_metadata:
            records.append({
                'Variable_Name': col,
                'Data_Type': str(df[col].dtype),
                'Description': column_metadata[col]['description'],
                'Source': column_metadata[col]['source'],
                'Notes': column_metadata[col]['notes']
            })
        else:
            records.append({
                'Variable_Name': col,
                'Data_Type': str(df[col].dtype),
                'Description': 'Additional variable',
                'Source': 'Derived',
                'Notes': ''
            })
    
    dictionary = pd.DataFrame(records)
    
    # Add value ranges
    ranges = []
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64']:
            ranges.append(f"{df[col].min():,.0f} to {df[col].max():,.0f}")
        else:
            unique_count = df[col].nunique()
            ranges.append(f"{unique_count} unique values")
    
    dictionary['Value_Range'] = ranges[:len(df.columns)]
    
    # Save data dictionary
    dict_file = DOCS_DIR / "data_dictionary.csv"
    dictionary.to_csv(dict_file, index=False)
    
    # Display
    print("\n  Data Dictionary Preview:")
    print(dictionary.to_string(index=False))
    
    return dictionary

# Save cleaned and standardized data
def save_cleaned_data(df):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save main cleaned file
    output_file = OUTPUT_DIR / "Elections_Cleaned_with_FIPS.csv"
    df.to_csv(output_file, index=False)
    print(f"\tSaved cleaned data: {output_file}")
    print(f"\tRows: {len(df)}")
    print(f"\tColumns: {len(df.columns)}")
    
    # Save summary by year
    summary_file = OUTPUT_DIR / "Turnout_Summary_by_Year.csv"
    summary = df.groupby('Year').agg({
        'County': 'count',
        'Registered_Voters': 'sum',
        'Votes_Cast': 'sum',
        'Turnout_Percent': ['mean', 'median', 'std', 'min', 'max']
    }).round(2)
    summary.columns = ['Counties', 'Total_Registered', 'Total_Votes', 
                       'Avg_Turnout', 'Median_Turnout', 'Std_Dev', 'Min_Turnout', 'Max_Turnout']
    summary.to_csv(summary_file)
    
    return output_file

def main():
    try:
        # Load data
        elections, fips = load_data()
        if elections is None:
            return
        
        # Standardize county names
        elections = standardize_county_names(elections)
        
        # Add FIPS codes
        elections = add_fips_codes(elections, fips)
        
        # Quality checks
        elections, issues = quality_checks(elections)
        
        # Summary statistics
        summary_stats = create_summary_statistics(elections)
        
        # Data dictionary
        data_dict = create_data_dictionary(elections)
        
        # Save results
        output_file = save_cleaned_data(elections)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()