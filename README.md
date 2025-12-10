# Florida Voter Turnout Analysis: A Data Curation Project

**Course:** CS 598 - Foundations of Data Curation  
**Author:** Dean Fletcher  
**Date:** December 2025

---

## Project Overview

This project creates a comprehensive, curated dataset integrating Florida election turnout data (2016-2024) with temporally-matched demographic and economic indicators to analyze factors influencing voter participation across 67 Florida counties. The curation process follows the Digital Curation Centre (DCC) Lifecycle Model, implementing systematic data acquisition, quality assessment, transformation, and preservation workflows.

### Research Question

How do demographic, socioeconomic, and geographic factors influence voter turnout patterns across Florida counties, and how have these relationships evolved across recent election cycles (2016-2024)?

### Final Dataset

- **File:** `data/processed/Master_Dataset_Temporal_Matched.csv`
- **Observations:** 335 (67 counties × 5 election years)
- **Variables:** 16
- **Missing Values:** 0

---

## Repository Structure

```
florida_voter_project/
├── data/
│   ├── raw/                          # Original unmodified source files
│   │   ├── elections/
│   │   │   └── election_results/     # Florida DOS turnout data (2016-2024)
│   │   ├── census/
│   │   │   └── acs_2020/             # ACS 5-year estimates (2016-2020)
│   │   ├── bea/
│   │   │   ├── income/               # CAINC1 personal income
│   │   │   └── gdp/                  # CAGDP2 GDP by county
│   │   ├── usda/
│   │   │   └── Ruralurbancontinuumcodes2023.csv
│   │   └── reference/
│   │       └── florida_fips_codes.csv
│   └── processed/                    # Cleaned and integrated datasets
│       ├── All_Elections_Combined_2016_2024.csv
│       ├── Elections_Cleaned_with_FIPS.csv
│       ├── Master_Dataset_Integrated.csv
│       └── Master_Dataset_Temporal_Matched.csv  # FINAL OUTPUT
├── scripts/
│   ├── combine_election_years.py     # Step 1: Combine election files
│   ├── clean_standardize.py          # Step 2: Clean and add FIPS codes
│   ├── data_integration.py           # Step 3: Initial Census/BEA integration
│   ├── add_usda_codes.py             # Step 4: Add geographic classification
│   └── temporal_matching.py          # Step 5: Year specific BEA matching
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

---

## Data Sources

### Florida Division of Elections
- **Source:** Florida Department of State
- **URL:** https://dos.myflorida.com/elections/data-statistics/
- **Temporal Coverage:** November 2016, 2018, 2020, 2022, 2024 general elections
- **Variables:** Registered voters, votes cast, turnout percentage
- **Acquisition:** Manual HTML table extraction (no bulk download available)
- **Update Frequency:** Published within 30 days post-certification

### U.S. Census Bureau - American Community Survey
- **Source:** U.S. Census Bureau
- **URL:** https://data.census.gov/
- **Dataset:** ACS 5-Year Estimates (2016-2020)
- **Tables Used:**
  - B19013: Median Household Income
  - B15003: Educational Attainment
  - B01003: Total Population
- **Format:** CSV via data.census.gov interface
- **Notes:** 5 year rolling estimates provide reliable county-level data

### Bureau of Economic Analysis
- **Source:** U.S. Department of Commerce, BEA
- **URL:** https://www.bea.gov/data/economic-accounts/regional
- **Tables Used:**
  - CAINC1: Personal Income (per capita)
  - CAGDP2: GDP by County
- **Temporal Coverage:** Annual data 2016-2023
- **Temporal Matching:** Year-specific matching to election years (2016→2016, 2018→2018, 2020→2020, 2022→2022, 2024→2024)
- **Release:** September 2024

### USDA Rural-Urban Continuum Codes
- **Source:** USDA Economic Research Service
- **URL:** https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/
- **Version:** 2023 codes (based on 2020 Census and 2023 OMB delineations)
- **Classification:** 9 categories based on metropolitan status and population size
- **Update Frequency:** Approximately every 10 years following decennial census

---

## Reproduction Instructions

### Prerequisites

- Python 3.11+
- pandas
- numpy

### Environment Setup

```bash
# Clone repository
git clone https://github.com/Dfletchh/CS-598-Foundations-of-Data-Curation.git
cd CS-598-Foundations-of-Data-Curation/florida_voter_project

# Install dependencies
pip install -r requirements.txt
```

### Execute Pipeline

Run scripts in order from the `scripts/` directory:

```bash
# Step 1: Combine election year files
python combine_election_years.py

# Step 2: Clean data and add FIPS codes
python clean_standardize.py

# Step 3: Initial Census and BEA integration
python data_integration.py

# Step 4: Add USDA Rural-Urban classification
python add_usda_codes.py

# Step 5: Implement year specific BEA temporal matching
python temporal_matching.py
```

Final output: `data/processed/Master_Dataset_Temporal_Matched.csv`

---

## Data Dictionary

| Variable | Type | Description | Source |
|----------|------|-------------|--------|
| FIPS | Integer | 5-digit Federal Information Processing Standard county code (12001-12133) | Census/DOS |
| County | String | Standardized county name | Florida DOS |
| County_Original | String | Original county name before standardization | Florida DOS |
| Year | Integer | Election year (2016, 2018, 2020, 2022, 2024) | Florida DOS |
| Election_Date | String | Date of general election (MM/DD/YYYY) | Florida DOS |
| Registered_Voters | Integer | Voters registered as of book closing (29 days pre-election) | Florida DOS |
| Votes_Cast | Integer | Total votes cast in general election | Florida DOS |
| Turnout_Percent | Float | (Votes_Cast / Registered_Voters) × 100 | Calculated |
| Median_Household_Income | Integer | Median household income in past 12 months (dollars) | Census ACS B19013 |
| Total_Population | Integer | Total county population | Census ACS B01003 |
| Pct_Bachelors_Plus | Float | Percentage of population 25+ with bachelor's degree or higher | Census ACS B15003 |
| Per_Capita_Income | Integer | Per capita personal income, year-specific (dollars) | BEA CAINC1 |
| GDP_Millions | Float | Gross Domestic Product in millions of current dollars, year-specific | BEA CAGDP2 |
| Rural_Urban_Code | Integer | USDA Rural-Urban Continuum Code (1-9) | USDA ERS 2023 |
| Rural_Urban_Description | String | Detailed description of RUCC category | Derived |
| Urban_Rural_Category | String | Simplified classification (Metropolitan/Micropolitan/Rural) | Derived |

---

## Quality Assessment

### Completeness
- All 67 Florida counties present in each of 5 election years (335 total observations)
- Zero missing values across all 16 variables
- 100% match rate for FIPS based joins

### Consistency
- Aggregated county totals validated against state-level published figures
- Calculated turnout percentages match reported values within rounding tolerance
- One minor discrepancy documented: DeSoto County 2018 (reported 53.7% vs calculated 56.6%)

### Temporal Validation
- Population and economic trends verified within plausible ranges
- BEA per capita income shows expected growth: $39,325 (2016) → $57,666 (2024), +46%

### Range Validation
- Turnout percentages: 40.5% to 94.1% (all within expected bounds)
- No duplicate FIPS year combinations

---

## Key Findings

1. **Presidential-Midterm Gap:** Presidential elections average 77.6% turnout vs 60.1% for midterms (17+ point differential)

2. **Socioeconomic Correlations:** Per capita income (+0.256) and education (+0.130) positively correlate with turnout

3. **Rural-Urban Differential:** Small rural counties (81.2% average) outperform large metropolitan areas (80.6%) despite lower incomes

4. **Economic Growth:** Year-specific BEA matching revealed 46% per capita income growth from 2016-2024

---

## Known Limitations

1. **ACS Temporal Alignment:** Census 2016-2020 estimates applied to all election years; demographic data exhibits gradual change, minimizing impact

2. **2024 BEA Data:** Uses 2023 estimates (most recent available) for 2024 election

3. **Employment Variable:** CAINC4 employment data excluded due to parsing complexity; per capita income and GDP provide robust economic measures

---

## References

Bureau of Economic Analysis. (2024). *Regional Economic Accounts*. U.S. Department of Commerce. https://www.bea.gov/data/economic-accounts/regional

Florida Division of Elections. (2024). *Election Results and Statistics*. Florida Department of State. https://dos.myflorida.com/elections/data-statistics/

Higgins, S. (2008). The DCC curation lifecycle model. *International Journal of Digital Curation, 3*(1), 134-140.

U.S. Census Bureau. (2024). *American Community Survey*. https://www.census.gov/programs-surveys/acs/

U.S. Department of Agriculture Economic Research Service. (2023). *Rural-Urban Continuum Codes*. https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/

Wickham, H. (2014). Tidy data. *Journal of Statistical Software, 59*(10), 1-23.
