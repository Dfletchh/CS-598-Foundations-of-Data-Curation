"""
Add USDA Rural-Urban Continuum Codes
Integrates rural-urban classification with master dataset
"""

import pandas as pd
from pathlib import Path

# Configuration
current_dir = Path.cwd()
if current_dir.name == "scripts":
    BASE_DIR = current_dir.parent
else:
    BASE_DIR = current_dir

print(f"Working from: {BASE_DIR}\n")

# Paths
MASTER_FILE = BASE_DIR / "data" / "processed" / "Master_Dataset_Integrated.csv"
USDA_FILE = BASE_DIR / "data" / "raw" / "usda" / "Ruralurbancontinuumcodes2023.csv"
OUTPUT_DIR = BASE_DIR / "data" / "processed"

def load_data():
    """Load master dataset and USDA codes"""
    print("=" * 70)
    print("ADD USDA RURAL-URBAN CODES")
    print("=" * 70)
    print("\n[1/4] Loading data...")
    
    # Load master dataset
    master = pd.read_csv(MASTER_FILE)
    print(f"Loaded master dataset: {len(master)} rows, {len(master.columns)} columns")
    
    # Load USDA codes
    try:
        usda = pd.read_csv(USDA_FILE, encoding='latin1')
        print(f"Loaded USDA codes: {len(usda)} rows (encoding: latin1)")
    except:
        try:
            usda = pd.read_csv(USDA_FILE, encoding='cp1252')
            print(f"Loaded USDA codes: {len(usda)} rows (encoding: cp1252)")
        except:
            usda = pd.read_csv(USDA_FILE, encoding='iso-8859-1')
            print(f"Loaded USDA codes: {len(usda)} rows (encoding: iso-8859-1)")
    
    print(f"  Columns: {list(usda.columns)}")
    
    return master, usda

def process_usda_codes(usda):
    """Process USDA rural-urban codes for Florida"""
    print("\n[2/4] Processing USDA codes...")
    
    # Filter for Florida (state FIPS = 12)
    usda['State_FIPS'] = usda['FIPS'].astype(str).str[:2]
    florida_usda = usda[usda['State_FIPS'] == '12'].copy()
    
    print(f"  Florida rows in USDA data: {len(florida_usda)}")
    
    # Check if data is in long format (Attribute/Value columns)
    if 'Attribute' in florida_usda.columns and 'Value' in florida_usda.columns:
        print(f"  Detected long format data")
        print(f"  Unique attributes: {florida_usda['Attribute'].unique()}")
        
        # Filter for RUCC attribute
        rucc_attributes = [attr for attr in florida_usda['Attribute'].unique() if 'RUCC' in str(attr)]
        if rucc_attributes:
            rucc_attr = rucc_attributes[0]
            print(f"  Using attribute: {rucc_attr}")
            
            # Filter and pivot
            florida_rucc = florida_usda[florida_usda['Attribute'] == rucc_attr].copy()
            florida_rucc = florida_rucc[['FIPS', 'County_Name', 'Value']].copy()
            florida_rucc.columns = ['FIPS', 'USDA_County_Name', 'Rural_Urban_Code']
            florida_rucc['Rural_Urban_Code'] = pd.to_numeric(florida_rucc['Rural_Urban_Code'], errors='coerce')
        else:
            print(f"  No RUCC attribute found in: {florida_usda['Attribute'].unique()}")
            return None
    else:
        # Wide format - original logic
        rucc_cols = [col for col in florida_usda.columns if 'RUCC' in col.upper()]
        if rucc_cols:
            rucc_col = rucc_cols[0]
            print(f"  Using column {rucc_col}")
        else:
            print(f"  No RUCC column found. Available columns: {list(florida_usda.columns)}")
            return None
        
        county_cols = [col for col in florida_usda.columns if 'county' in col.lower() or 'name' in col.lower()]
        county_col = county_cols[0] if county_cols else 'County_Name'
        
        florida_rucc = florida_usda[['FIPS', rucc_col, county_col]].copy()
        florida_rucc.columns = ['FIPS', 'Rural_Urban_Code', 'USDA_County_Name']
    
    print(f"  Florida counties with RUCC codes: {len(florida_rucc)}")
    
    # Add categorical descriptions
    rucc_descriptions = {
        1: 'Metro - Large (1M+ pop)',
        2: 'Metro - Medium (250K-1M pop)',
        3: 'Metro - Small (<250K pop)',
        4: 'Nonmetro - Urban (20K+, adjacent to metro)',
        5: 'Nonmetro - Urban (20K+, not adjacent)',
        6: 'Nonmetro - Urban (2.5-20K, adjacent to metro)',
        7: 'Nonmetro - Urban (2.5-20K, not adjacent)',
        8: 'Nonmetro - Rural (<2.5K, adjacent to metro)',
        9: 'Nonmetro - Rural (<2.5K, not adjacent)'
    }
    
    florida_rucc['Rural_Urban_Description'] = florida_rucc['Rural_Urban_Code'].map(rucc_descriptions)
    
    # Create simplified categories
    def categorize(code):
        if pd.isna(code):
            return None
        if code in [1, 2, 3]:
            return 'Metropolitan'
        elif code in [4, 5, 6, 7]:
            return 'Micropolitan/Small Urban'
        else:  # 8, 9
            return 'Rural'
    
    florida_rucc['Urban_Rural_Category'] = florida_rucc['Rural_Urban_Code'].apply(categorize)
    
    print("  Rural-Urban Distribution:")
    for category, count in florida_rucc['Urban_Rural_Category'].value_counts().items():
        print(f"    {category}: {count} counties")
    
    return florida_rucc[['FIPS', 'Rural_Urban_Code', 'Rural_Urban_Description', 'Urban_Rural_Category']]

def integrate_usda(master, usda_codes):
    """Integrate USDA codes with master dataset"""
    print("\n[3/4] Integrating USDA codes with master dataset...")
    
    # Merge on FIPS
    integrated = master.merge(usda_codes, on='FIPS', how='left')
    
    # Check for unmatched
    unmatched = integrated[integrated['Rural_Urban_Code'].isna()]
    if len(unmatched) > 0:
        print(f"  Warning: {len(unmatched)} records not matched")
        print(f"    Unmatched counties: {unmatched['County'].unique()}")
    else:
        print(f"  All records matched successfully")
    
    print(f"\n  Final integrated dataset:")
    print(f"    Rows: {len(integrated)}")
    print(f"    Columns: {len(integrated.columns)}")
    print(f"    New columns: Rural_Urban_Code, Rural_Urban_Description, Urban_Rural_Category")
    
    return integrated

def analyze_rural_urban_patterns(df):
    """Analyze turnout patterns by rural-urban classification"""
    print("\n[4/4] Analyzing rural-urban turnout patterns...")
    
    # Focus on 2024 data
    df_2024 = df[df['Year'] == 2024].copy()
    
    print("\n  2024 Average Turnout by Urban-Rural Category:")
    turnout_by_category = df_2024.groupby('Urban_Rural_Category')['Turnout_Percent'].agg(['mean', 'std', 'count'])
    turnout_by_category.columns = ['Avg_Turnout', 'Std_Dev', 'N_Counties']
    turnout_by_category['Avg_Turnout'] = turnout_by_category['Avg_Turnout'].round(2)
    turnout_by_category['Std_Dev'] = turnout_by_category['Std_Dev'].round(2)
    print(turnout_by_category.to_string())
    
    print("\n  Top 5 Counties by Category (2024):")
    for category in df_2024['Urban_Rural_Category'].unique():
        if pd.notna(category):
            print(f"\n  {category}:")
            top5 = df_2024[df_2024['Urban_Rural_Category'] == category].nlargest(5, 'Turnout_Percent')[
                ['County', 'Turnout_Percent', 'Total_Population']
            ]
            for _, row in top5.iterrows():
                print(f"    {row['County']}: {row['Turnout_Percent']:.1f}% (pop: {row['Total_Population']:,.0f})")

def save_data(df):
    """Save updated dataset"""
    print("\n[5/5] Saving updated dataset...")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save complete dataset
    output_file = OUTPUT_DIR / "Master_Dataset_with_USDA.csv"
    df.to_csv(output_file, index=False)
    print(f"  Saved: {output_file}")
    
    # Save 2024 subset
    df_2024 = df[df['Year'] == 2024]
    output_2024 = OUTPUT_DIR / "Analysis_Dataset_2024_with_USDA.csv"
    df_2024.to_csv(output_2024, index=False)
    print(f"  Saved 2024 subset: {output_2024}")
    
    # Update data dictionary
    print("\n  Updated variables:")
    print("    • Rural_Urban_Code (1-9): USDA classification code")
    print("    • Rural_Urban_Description: Detailed description of classification")
    print("    • Urban_Rural_Category: Simplified (Metropolitan/Micropolitan/Rural)")
    
    return output_file

def main():
    """Main execution"""
    try:
        # Load data
        master, usda = load_data()
        
        # Process USDA codes
        usda_codes = process_usda_codes(usda)
        
        # Integrate
        integrated = integrate_usda(master, usda_codes)
        
        # Analyze patterns
        analyze_rural_urban_patterns(integrated)
        
        # Save
        output_file = save_data(integrated)
        
        # Summary
        print("\n" + "=" * 70)
        print("COMPLETE!")
        print("\n" + "=" * 70)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
