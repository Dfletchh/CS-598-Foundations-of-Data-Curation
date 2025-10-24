"""
Processes Census and BEA data and integrates with election data
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuration
current_dir = Path.cwd()
if current_dir.name == "scripts":
    BASE_DIR = current_dir.parent
else:
    BASE_DIR = current_dir

# Paths
ELECTION_FILE = BASE_DIR / "data" / "processed" / "Elections_Cleaned_with_FIPS.csv"
CENSUS_DIR = BASE_DIR / "data" / "raw" / "census" / "acs_2020"
BEA_DIR = BASE_DIR / "data" / "raw" / "bea"
OUTPUT_DIR = BASE_DIR / "data" / "processed"

# Load cleaned election data
def load_election_data():
    print("=" * 70)
    print("STEP 3: DATA INTEGRATION")
    print("=" * 70)
    print("\n[1/6] Loading election data...")
    
    elections = pd.read_csv(ELECTION_FILE)
    print(f"  Loaded election data: {len(elections)} rows")
    print(f"  Years: {sorted(elections['Year'].unique())}")
    print(f"  Counties: {elections['County'].nunique()}")
    
    return elections

# Process Census ACS data
def process_census_data():
    census_dfs = {}
    
    # Median Household Income
    try:
        income = pd.read_csv(CENSUS_DIR / "median_household_income_2020.csv")
        if 'GEO_ID' in income.columns:
            income = income[income['GEO_ID'].str.startswith('0500000US12', na=False)]
            income['FIPS'] = income['GEO_ID'].str[-5:].astype(int)
            
            # Look for estimate column
            income_col = [col for col in income.columns if 'B19013' in col and col.endswith('E')][0]
            income = income[['FIPS', income_col]].rename(columns={income_col: 'Median_Household_Income'})
            income['Median_Household_Income'] = pd.to_numeric(income['Median_Household_Income'], errors='coerce')
            census_dfs['income'] = income
            print(f"\tProcessed: {len(income)} counties")
    except Exception as e:
        print(f"\tError: {e}")
    
    # Total Population
    try:
        pop = pd.read_csv(CENSUS_DIR / "total_population_2020.csv")
        if 'GEO_ID' in pop.columns:
            pop = pop[pop['GEO_ID'].str.startswith('0500000US12', na=False)]
            pop['FIPS'] = pop['GEO_ID'].str[-5:].astype(int)
            pop_col = [col for col in pop.columns if 'B01003' in col and col.endswith('E')][0]
            pop = pop[['FIPS', pop_col]].rename(columns={pop_col: 'Total_Population'})
            pop['Total_Population'] = pd.to_numeric(pop['Total_Population'], errors='coerce')
            census_dfs['population'] = pop
            print(f"\tProcessed: {len(pop)} counties")
    except Exception as e:
        print(f"\tError: {e}")
    
    # Educational Attainment
    try:
        edu = pd.read_csv(CENSUS_DIR / "educational_attainment_2020.csv")
        if 'GEO_ID' in edu.columns:
            edu = edu[edu['GEO_ID'].str.startswith('0500000US12', na=False)]
            edu['FIPS'] = edu['GEO_ID'].str[-5:].astype(int)
            
            # Total population 25+
            total_col = [col for col in edu.columns if 'B15003_001' in col and col.endswith('E')]
            # Bachelor's degree
            bach_col = [col for col in edu.columns if 'B15003_022' in col and col.endswith('E')]
            # Master's degree
            mast_col = [col for col in edu.columns if 'B15003_023' in col and col.endswith('E')]
            # Professional degree
            prof_col = [col for col in edu.columns if 'B15003_024' in col and col.endswith('E')]
            # Doctorate
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
                census_dfs['education'] = edu
                print(f"\tProcessed: {len(edu)} counties")
    except Exception as e:
        print(f"\tError: {e}")
    
    # Median Age from Sex by Age
    try:
        age = pd.read_csv(CENSUS_DIR / "sex_by_age_2020.csv")
        if 'GEO_ID' in age.columns:
            age = age[age['GEO_ID'].str.startswith('0500000US12', na=False)]
            age['FIPS'] = age['GEO_ID'].str[-5:].astype(int)
            
            # Find median age column
            median_age_col = [col for col in age.columns if 'B01002_001' in col and col.endswith('E')]
            if median_age_col:
                age = age[['FIPS', median_age_col[0]]].rename(columns={median_age_col[0]: 'Median_Age'})
                age['Median_Age'] = pd.to_numeric(age['Median_Age'], errors='coerce')
                census_dfs['age'] = age
                print(f"\tProcessed: {len(age)} counties")
    except Exception as e:
        print(f"\tError: {e}")
    
    # Combine all census data
    if census_dfs:
        census_combined = census_dfs['income'] if 'income' in census_dfs else None
        
        for key in ['population', 'education', 'age']:
            if key in census_dfs:
                if census_combined is not None:
                    census_combined = census_combined.merge(census_dfs[key], on='FIPS', how='outer')
                else:
                    census_combined = census_dfs[key]
        
        print(f"\tCombined Census data: {len(census_combined)} counties, {len(census_combined.columns)-1} variables")
        return census_combined
    else:
        return None

# Process BEA economic data
def process_bea_data():
    bea_dfs = {}
    
    # Personal Income (CAINC1)
    try:
        income_file = BEA_DIR / "personal_income" / "CAINC1_FL_1969_2023.csv"
        income = pd.read_csv(income_file, encoding='latin1')
        
        # Clean GeoFIPS - remove quotes and spaces
        income['GeoFIPS_clean'] = income['GeoFIPS'].str.strip().str.strip('"').str.strip()
        
        # Filter for Florida counties
        income = income[income['GeoFIPS_clean'].str.len() == 5]
        income = income[income['GeoFIPS_clean'].str.startswith('12')]
        income = income[income['GeoFIPS_clean'] != '12000']  # Exclude state total
        income['FIPS'] = income['GeoFIPS_clean'].astype(int)
        
        # Get most recent year available
        year_cols = [col for col in income.columns if col.isdigit()]
        latest_year = max(year_cols)
        
        # Filter for Per Capita Personal Income line
        income['Description_clean'] = income['Description'].str.strip()
        income = income[income['Description_clean'].str.contains('Per capita personal income', case=False, na=False)]
        
        income = income[['FIPS', latest_year]].rename(columns={latest_year: 'Per_Capita_Income'})
        income['Per_Capita_Income'] = pd.to_numeric(income['Per_Capita_Income'], errors='coerce')
        
        bea_dfs['income'] = income
        print(f"\tProcessed: {len(income)} counties ({latest_year} data)")
    except Exception as e:
        print(f"\tError: {e}")
        import traceback
        traceback.print_exc()
    
    # GDP (CAGDP2)
    try:
        gdp_file = BEA_DIR / "gdp" / "CAGDP2_FL_2001_2023.csv"
        gdp = pd.read_csv(gdp_file, encoding='latin1')
        
        # Clean GeoFIPS
        gdp['GeoFIPS_clean'] = gdp['GeoFIPS'].str.strip().str.strip('"').str.strip()
        
        # Filter for counties
        gdp = gdp[gdp['GeoFIPS_clean'].str.len() == 5]
        gdp = gdp[gdp['GeoFIPS_clean'].str.startswith('12')]
        gdp = gdp[gdp['GeoFIPS_clean'] != '12000']
        gdp['FIPS'] = gdp['GeoFIPS_clean'].astype(int)
        
        year_cols = [col for col in gdp.columns if col.isdigit()]
        latest_year = max(year_cols)
        
        # Filter for total GDP
        gdp['Description_clean'] = gdp['Description'].str.strip()
        gdp = gdp[gdp['Description_clean'].str.contains('All industry total', case=False, na=False)]
        
        gdp = gdp[['FIPS', latest_year]].rename(columns={latest_year: 'GDP_Millions'})
        gdp['GDP_Millions'] = pd.to_numeric(gdp['GDP_Millions'], errors='coerce')
        
        bea_dfs['gdp'] = gdp
        print(f"\tProcessed: {len(gdp)} counties ({latest_year} data)")
    except Exception as e:
        print(f"\tError: {e}")
        import traceback
        traceback.print_exc()
    
    # Employment (CAINC4)
    try:
        emp_file = BEA_DIR / "employment" / "CAINC4_FL_1969_2023.csv"
        emp = pd.read_csv(emp_file, encoding='latin1')
        
        # Clean GeoFIPS
        emp['GeoFIPS_clean'] = emp['GeoFIPS'].str.strip().str.strip('"').str.strip()
        
        # Filter for counties
        emp = emp[emp['GeoFIPS_clean'].str.len() == 5]
        emp = emp[emp['GeoFIPS_clean'].str.startswith('12')]
        emp = emp[emp['GeoFIPS_clean'] != '12000']
        emp['FIPS'] = emp['GeoFIPS_clean'].astype(int)
        
        year_cols = [col for col in emp.columns if col.isdigit()]
        latest_year = max(year_cols)
        
        # Filter for total employment line
        emp['Description_clean'] = emp['Description'].str.strip()
        # CAINC4 has "Total employment" in the description
        emp = emp[emp['Description_clean'].str.contains('Total employment', case=False, na=False)]
        
        emp = emp[['FIPS', latest_year]].rename(columns={latest_year: 'Total_Employment'})
        emp['Total_Employment'] = pd.to_numeric(emp['Total_Employment'], errors='coerce')
        
        bea_dfs['employment'] = emp
        print(f"\tProcessed: {len(emp)} counties ({latest_year} data)")
    except Exception as e:
        print(f"\tError: {e}")
        import traceback
        traceback.print_exc()
    
    # Combine BEA data
    if bea_dfs:
        bea_combined = bea_dfs['income'] if 'income' in bea_dfs else None
        
        for key in ['gdp', 'employment']:
            if key in bea_dfs:
                if bea_combined is not None:
                    bea_combined = bea_combined.merge(bea_dfs[key], on='FIPS', how='outer')
                else:
                    bea_combined = bea_dfs[key]
        
        print(f"\tCombined BEA data: {len(bea_combined)} counties, {len(bea_combined.columns)-1} variables")
        return bea_combined
    else:
        print("\tNo BEA data processed")
        return None

# Integrate election, census, and BEA data
def integrate_all_data(elections, census, bea):
    # election data
    integrated = elections.copy()
    
    # Add Census data
    if census is not None:
        integrated = integrated.merge(census, on='FIPS', how='left')
    
    # Add BEA data
    if bea is not None:
        integrated = integrated.merge(bea, on='FIPS', how='left')
    
    print(f"\n  Final integrated dataset:")
    print(f"    Rows: {len(integrated)}")
    print(f"    Columns: {len(integrated.columns)}")
    print(f"    Variables: {', '.join(integrated.columns)}")
    
    return integrated

# Create summary statistics for integrated data
def create_analysis_summary(df):
    print("\n  Data Completeness:")
    total_records = len(df)
    for col in df.columns:
        if col not in ['County', 'Election_Date', 'County_Original']:
            missing = df[col].isnull().sum()
            pct_complete = ((total_records - missing) / total_records * 100)
            status = "Pass" if pct_complete == 100 else "Fail"
            print(f"\t{status} {col}: {pct_complete:.1f}% complete ({total_records - missing}/{total_records})")
    
    # Correlation between demographics and 2024 turnout
    print("\n  Correlation check (2024 election):")
    df_2024 = df[df['Year'] == 2024].copy()
    
    numeric_cols = df_2024.select_dtypes(include=[np.number]).columns
    demo_vars = [col for col in numeric_cols if col not in ['Year', 'FIPS', 'Registered_Voters', 'Votes_Cast', 'Turnout_Percent']]
    
    if len(demo_vars) > 0:
        try:
            # Calculate correlations only for columns with data
            corr_data = df_2024[demo_vars + ['Turnout_Percent']].dropna(axis=1, how='all')
            if 'Turnout_Percent' in corr_data.columns and len(corr_data.columns) > 1:
                corr_matrix = corr_data.corr()
                corr_with_turnout = corr_matrix['Turnout_Percent'].drop('Turnout_Percent')
                
                print("\tCorrelation with Turnout:")
                for var in corr_with_turnout.sort_values(ascending=False).index:
                    corr = corr_with_turnout[var]
                    if not pd.isna(corr):
                        print(f"      {var}: {corr:.3f}")
            else:
                print("\tNo complete demographic variables available for correlation")
        except Exception as e:
            print(f"\tCould not calculate correlations: {e}")
    else:
        print("\tNo demographic variables available")

# Save integrated dataset
def save_integrated_data(df):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    output_file = OUTPUT_DIR / "Master_Dataset_Integrated.csv"
    df.to_csv(output_file, index=False)
    
    print(f"\tSaved: {output_file}")
    print(f"\tRows: {len(df)}")
    print(f"\tColumns: {len(df.columns)}")
    
    # Save a 2024-only file for sample analysis
    df_2024 = df[df['Year'] == 2024]
    output_2024 = OUTPUT_DIR / "Analysis_Dataset_2024.csv"
    df_2024.to_csv(output_2024, index=False)
    print(f"\tSaved 2024 sample: {output_2024}")
    
    return output_file

def main():
    try:
        # Load election data
        elections = load_election_data()
        
        # Process Census data
        census = process_census_data()
        
        # Process BEA data
        bea = process_bea_data()
        
        # Integrate all data
        integrated = integrate_all_data(elections, census, bea)
        
        # Create summary
        create_analysis_summary(integrated)
        
        # Save results
        output_file = save_integrated_data(integrated)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()