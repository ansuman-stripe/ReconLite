# ReconLite
Automating cash recon processes to resolve unreconciled PBATs related to incident `ir_construct_omniscient`, reducing manual TOIL.

## About the Code
This script automates the reconciliation of bank transactions by processing data from multiple sources and generating actionable outputs. When executed, the script creates up to 6 CSV files. On subsequent runs, it updates the `main_data.csv` file with the latest data provided by Wells Fargo and refreshes the other 5 output files with current processing results.

### Output Files

#### 1. `main_data.csv`
- **Purpose**: Serves as a persistent data store for all VBAN (Virtual Bank Account Number) information.
- **Contents**: Mapping of reference numbers to corresponding VBANs.
- **Update Pattern**: Continuously appended with new unique entries from Wells Fargo reports.

#### 2. `merged_data.csv`
- **Purpose**: Comprehensive dataset for audit and record-keeping.
- **Contents**: Complete joined data with details on how PBATs were processed, including transaction details, customer/merchant information, and processing status.
- **Use Case**: Can be imported into tracking spreadsheets for thorough documentation and analysis.

#### 3. `jiraUploadData.csv`
- **Purpose**: Contains essential data for creating JIRA tickets.
- **Contents**: Raw filtered data of all processed PBATs before any column transformations.
- **Use Case**: Streamlines the creation of JIRA issues for tracking reconciliation tasks.

#### 4. `generateSynthticIBAT_data.csv`
- **Purpose**: Ready-to-use input file for the "Generate Synthetic IBAT" Excelsior process.
- **Contents**: Formatted data for PBATs with VBANs on Horizon rails, including:
  - PBAT IDs
  - JIRA links
  - VBAN account numbers
  - Configuration flags for wire reference handling
- **Use Case**: Direct import into Excelsior to create synthetic IBATs.

#### 5. `manuallyUpdateWireDescription_data.csv`
- **Purpose**: Ready-to-use input file for the "Manually Update Wire Description" Excelsior process.
- **Contents**: Formatted data for PBATs with sources on Payserver rails, including:
  - PBAT and IBAT identifiers
  - Source references
  - Configuration flags for wire reference formatting
- **Use Case**: Direct import into Excelsior to update wire descriptions.

#### 6. `exceptionCases_data.csv`
- **Purpose**: Captures cases that couldn't be automatically processed.
- **Contents**: PBATs with anomalies such as:
  - Missing VBAN information
  - Rejected merchant accounts
  - Consumed sources
  - Other processing exceptions
- **Use Case**: Enables manual review and intervention for special cases.

## Workflow

1. **Data Acquisition**:
   - Retrieves transaction data from Hubble queries
   - Imports latest Wells Fargo reports with VBAN information
   - Maintains persistent storage of VBANs in `main_data.csv`

2. **Data Processing**:
   - Merges transaction data with VBANs
   - Joins with customer, source, and merchant information
   - Applies business logic to categorize transactions

3. **Output Generation**:
   - Filters and formats data for different use cases
   - Creates specialized CSV files for each downstream process
   - Identifies exceptions requiring manual intervention

## Usage

1. Run the script when new Wells Fargo reports are available
2. Enter the JIRA link when prompted (optional)
3. Use the generated CSV files for their respective processes
4. Review exception cases for manual handling

This automation significantly reduces the time and effort required for reconciling bank transactions, ensuring consistent processing and comprehensive documentation.