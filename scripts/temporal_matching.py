"""
Step 5: Temporal Matching for BEA Data
Matches BEA economic data to specific election years
Addresses instructor feedback about using year-specific data
"""

import pandas as pd
from pathlib import Path
import numpy as np

# Configuration
current_dir = Path.cwd()
if current_dir.name == "scripts":
    BASE_DIR = current_dir.parent
else:
    BASE_DIR = current_dir

print(f"Working from: {BASE_DIR}\n")

# Paths
ELECTION_FILE = BASE_DIR / "data" / "processed" / "Elections_Cleaned_with_FIPS.csv"
CENSUS_DIR = BASE_DIR / "data" / "raw" / "census" / "acs_2020"
BEA_DIR = BASE_DIR / "data" / "raw" / "bea"
USDA_FILE = BASE_DIR / "data" / "raw" / "usda" / "Ruralurbancontinuumcodes2023.csv"
OUTPUT_DIR = BASE_DIR / "data" / "processed"

def load_election_data():
    """Load election data"""
    print("=" * 70)
    print("TEMPORAL MATCHING - BEA DATA")
    print("=" * 70)
    print("\n[1/5] Loading election data...")
    
    elections = pd.read_csv(ELECTION_FILE)
    print(f"Loaded election data: {len(elections)} rows")
    print(f"  Years: {sorted(elections['Year'].unique())}")
    
    return elections

def process_bea_temporal(elections):
    """Process BEA data with year-specific matching"""
    print("\n[2/5] Processing BEA data with temporal matching...")
    
    # Mapping election years to BEA data years
    year_mapping = {
        2016: '2016',
        2018: '2018',
        2020: '2020',
        2022: '2022',
        2024: '2023'
    }
    
    all_bea_data = []
    
    for election_year, bea_year in year_mapping.items():
        print(f"\n  Processing {election_year} election → {bea_year} BEA data:")
        
        # Get elections for this year
        year_elections = elections[elections['Year'] == election_year].copy()
        
        # Process Personal Income
        try:
            income_file = BEA_DIR / "personal_income" / "CAINC1_FL_1969_2023.csv"
            income = pd.read_csv(income_file, encoding='latin1')
            
            # Clean and filter
            income['GeoFIPS_clean'] = income['GeoFIPS'].str.strip().str.strip('"').str.strip()
            income = income[income['GeoFIPS_clean'].str.len() == 5]
            income = income[income['GeoFIPS_clean'].str.startswith('12')]
            income = income[income['GeoFIPS_clean'] != '12000']
            income['FIPS'] = income['GeoFIPS_clean'].astype(int)
            
            # Filter for Per Capita Income line
            income['Description_clean'] = income['Description'].str.strip()
            income = income[income['Description_clean'].str.contains('Per capita personal income', case=False, na=False)]
            
            # Extract year-specific data
            if bea_year in income.columns:
                income = income[['FIPS', bea_year]].rename(columns={bea_year: 'Per_Capita_Income'})
                income['Per_Capita_Income'] = pd.to_numeric(income['Per_Capita_Income'], errors='coerce')
                print(f"    Per Capita Income: {len(income)} counties")
            else:
                print(f"    Year {bea_year} not found in income file")
                income = None
        except Exception as e:
            print(f"    Income error: {e}")
            income = None
        
        # Process GDP
        try:
            gdp_file = BEA_DIR / "gdp" / "CAGDP2_FL_2001_2023.csv"
            gdp = pd.read_csv(gdp_file, encoding='latin1')
            
            # Clean and filter
            gdp['GeoFIPS_clean'] = gdp['GeoFIPS'].str.strip().str.strip('"').str.strip()
            gdp = gdp[gdp['GeoFIPS_clean'].str.len() == 5]
            gdp = gdp[gdp['GeoFIPS_clean'].str.startswith('12')]
            gdp = gdp[gdp['GeoFIPS_clean'] != '12000']
            gdp['FIPS'] = gdp['GeoFIPS_clean'].astype(int)
            
            # Filter for All Industry Total
            gdp['Description_clean'] = gdp['Description'].str.strip()
            gdp = gdp[gdp['Description_clean'].str.contains('All industry total', case=False, na=False)]
            
            # Extract year-specific data
            if bea_year in gdp.columns:
                gdp = gdp[['FIPS', bea_year]].rename(columns={bea_year: 'GDP_Millions'})
                gdp['GDP_Millions'] = pd.to_numeric(gdp['GDP_Millions'], errors='coerce')
                print(f"    GDP: {len(gdp)} counties")
            else:
                print(f"    Year {bea_year} not found in GDP file")
                gdp = None
        except Exception as e:
            print(f"    GDP error: {e}")
            gdp = None
        
        # Merge BEA data with elections for this year
        year_data = year_elections.copy()
        if income is not None:
            year_data = year_data.merge(income, on='FIPS', how='left')
        if gdp is not None:
            year_data = year_data.merge(gdp, on='FIPS', how='left')
        
        all_bea_data.append(year_data)
        print(f"    Integrated {election_year} data")
    
    # Combine all years
    combined = pd.concat(all_bea_data, ignore_index=True)
    print(f"\n  Combined all years: {len(combined)} observations")
    
    return combined

def add_census_data(df):
    """Add Census ACS 2016-2020 data (same as before)"""
    print("\n[3/5] Adding Census ACS data...")
    
    census_vars = {}
    
    # Median Household Income
    try:
        income = pd.read_csv(CENSUS_DIR / "median_household_income_2020.csv")
        if 'GEO_ID' in income.columns:
            income = income[income['GEO_ID'].str.startswith('0500000US12', na=False)]
            income['FIPS'] = income['GEO_ID'].str[-5:].astype(int)
            income_col = [col for col in income.columns if 'B19013' in col and col.endswith('E')][0]
            income = income[['FIPS', income_col]].rename(columns={income_col: 'Median_Household_Income'})
            income['Median_Household_Income'] = pd.to_numeric(income['Median_Household_Income'], errors='coerce')
            census_vars['income'] = income
            print(f"  Median Income: {len(income)} counties")
    except Exception as e:
        print(f"  Income error: {e}")
    
    # Total Population
    try:
        pop = pd.read_csv(CENSUS_DIR / "total_population_2020.csv")
        if 'GEO_ID' in pop.columns:
            pop = pop[pop['GEO_ID'].str.startswith('0500000US12', na=False)]
            pop['FIPS'] = pop['GEO_ID'].str[-5:].astype(int)
            pop_col = [col for col in pop.columns if 'B01003' in col and col.endswith('E')][0]
            pop = pop[['FIPS', pop_col]].rename(columns={pop_col: 'Total_Population'})
            pop['Total_Population'] = pd.to_numeric(pop['Total_Population'], errors='coerce')
            census_vars['population'] = pop
            print(f"  Population: {len(pop)} counties")
    except Exception as e:
        print(f"  Population error: {e}")
    
    # Educational Attainment
    try:
        edu = pd.read_csv(CENSUS_DIR / "educational_attainment_2020.csv")
        if 'GEO_ID' in edu.columns:
            edu = edu[edu['GEO_ID'].str.startswith('0500000US12', na=False)]
            edu['FIPS'] = edu['GEO_ID'].str[-5:].astype(int)
            
            total_col = [col for col in edu.columns if 'B15003_001' in col and col.endswith('E')]
            bach_col = [col for col in edu.columns if 'B15003_022' in col and col.endswith('E')]
            mast_col = [col for col in edu.columns if 'B15003_023' in col and col.endswith('E')]
            prof_col = [col for col in edu.columns if 'B15003_024' in col and col.endswith('E')]
            doct_col = [col for col in edu.columns if 'B15003_025' in col and col.endswith('E')]
            
            if total_col and bach_col:
                edu['Total_25plus'] = pd.to_numeric(edu[total_col[0]], errors='coerce')
                edu['Bachelors_Plus'] = (
                    pd.to_numeric(edu[bach_col[0]], errors='coerce') +
                    pd.to_numeric(edu[mast_col[0]] if mast_col else 0, errors='coerce') +
                    pd.to_numeric(edu[prof_col[0]] if prof_col else 0, errors='coerce') +
                    pd.to_numeric(edu[doct_col[0]] if doct_col else 0, errors='coerce')
                )
                edu['Pct_Bachelors_Plus'] = (edu['Bachelors_Plus'] / edu['Total_25plus'] * 100).round(2)
                edu = edu[['FIPS', 'Pct_Bachelors_Plus']]
                census_vars['education'] = edu
                print(f"  Education: {len(edu)} counties")
    except Exception as e:
        print(f"  Education error: {e}")
    
    # Merge all Census variables
    for key, data in census_vars.items():
        df = df.merge(data, on='FIPS', how='left')
    
    return df

def add_usda_codes(df):
    """Add USDA Rural-Urban codes"""
    print("\n[4/5] Adding USDA Rural-Urban codes...")
    
    try:
        usda = pd.read_csv(USDA_FILE, encoding='latin1')
        usda['State_FIPS'] = usda['FIPS'].astype(str).str[:2]
        florida_usda = usda[usda['State_FIPS'] == '12'].copy()
        
        # Filter for RUCC attribute
        florida_rucc = florida_usda[florida_usda['Attribute'] == 'RUCC_2023'].copy()
        florida_rucc = florida_rucc[['FIPS', 'Value']].copy()
        florida_rucc.columns = ['FIPS', 'Rural_Urban_Code']
        florida_rucc['Rural_Urban_Code'] = pd.to_numeric(florida_rucc['Rural_Urban_Code'], errors='coerce')
        
        # Add descriptions and categories
        rucc_descriptions = {
            1: 'Metro - Large (1M+ pop)', 2: 'Metro - Medium (250K-1M pop)', 
            3: 'Metro - Small (<250K pop)', 4: 'Nonmetro - Urban (20K+, adjacent to metro)',
            5: 'Nonmetro - Urban (20K+, not adjacent)', 6: 'Nonmetro - Urban (2.5-20K, adjacent to metro)',
            7: 'Nonmetro - Urban (2.5-20K, not adjacent)', 8: 'Nonmetro - Rural (<2.5K, adjacent to metro)',
            9: 'Nonmetro - Rural (<2.5K, not adjacent)'
        }
        florida_rucc['Rural_Urban_Description'] = florida_rucc['Rural_Urban_Code'].map(rucc_descriptions)
        
        def categorize(code):
            if pd.isna(code): return None
            if code in [1, 2, 3]: return 'Metropolitan'
            elif code in [4, 5, 6, 7]: return 'Micropolitan/Small Urban'
            else: return 'Rural'
        
        florida_rucc['Urban_Rural_Category'] = florida_rucc['Rural_Urban_Code'].apply(categorize)
        
        # Merge
        df = df.merge(florida_rucc, on='FIPS', how='left')
        print(f"  Added USDA codes: {len(florida_rucc)} counties matched")
        
    except Exception as e:
        print(f"  USDA error: {e}")
    
    return df

def save_and_summarize(df):
    """Save final dataset and create summary"""
    print("\n[5/5] Saving temporally-matched dataset...")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save complete dataset
    output_file = OUTPUT_DIR / "Master_Dataset_Temporal_Matched.csv"
    df.to_csv(output_file, index=False)
    
    print(f"  Saved: {output_file}")
    print(f"    Rows: {len(df)}")
    print(f"    Columns: {len(df.columns)}")
    
    # Data completeness check
    print("\n  Data Completeness by Variable:")
    for col in df.columns:
        if col not in ['County', 'Election_Date', 'County_Original', 'USDA_County_Name']:
            missing = df[col].isnull().sum()
            pct = ((len(df) - missing) / len(df) * 100)
            status = "✓" if pct == 100 else "⚠"
            print(f"    {status} {col}: {pct:.1f}%")
    
    # Show temporal variation
    print("\n  BEA Data Temporal Variation (Per Capita Income):")
    summary = df.groupby('Year')['Per_Capita_Income'].agg(['mean', 'std']).round(0)
    summary.columns = ['Mean', 'Std_Dev']
    print(summary.to_string())
    
    return output_file

def main():
    """Main execution"""
    try:
        # Load elections
        elections = load_election_data()
        
        # Process BEA with temporal matching
        df = process_bea_temporal(elections)
        
        # Add Census data
        df = add_census_data(df)
        
        # Add USDA codes
        df = add_usda_codes(df)
        
        # Save and summarize
        output_file = save_and_summarize(df)
        
        # Final summary
        print("\n" + "=" * 70)
        print("TEMPORAL MATCHING COMPLETE!")
        print("\n" + "=" * 70)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
