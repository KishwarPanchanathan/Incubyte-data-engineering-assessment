# SkyPoints – Airline Loyalty Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline for an airline loyalty platform using **Databricks Free Edition, PySpark, Delta Lake and SQL**.

The pipeline processes member profile data received as country-specific CSV files and mileage redemption transactions received in nested JSON format.

It follows the Medallion Architecture (Bronze, Silver and Gold) to ingest, standardize, validate, transform and publish business-ready datasets.

## Business Scenario

SkyPoints receives:
- Daily member profile files from India, USA and Australia.
- Semi-structured JSON feeds containing mileage redemption transactions.
- Member information that may change over time, including country information.

The objective is to build a reliable pipeline that handles different source formats, validates data quality, derives business attributes and prepares member and redemption datasets for downstream analytics.

## Architecture

```text
       SOURCE SYSTEMS
             |
       +-----+------+
       |            |
    CSV Files    JSON Feed
       |            |
       +-----+------+
             |
        BRONZE LAYER
      Raw Delta Tables
             |
             v
        SILVER LAYER
   Standardization & Validation
   - Schema normalization
   - Date parsing
   - Age calculation
   - Stale member identification
   - Invalid record handling
             |
             v
         GOLD LAYER
   Business-Ready Delta Tables
   - Consolidated member master
   - India member table
   - USA member table
   - Australia member table
             |
             v
     REDEMPTION PROCESSING
   - JSON parsing
   - Array explosion
   - Transaction flattening
   - Data quality validation
             |
             v
      END-TO-END TESTING
   - Record reconciliation
   - Duplicate checks
   - Mandatory field checks
   - Business rule validation
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Development environment |
| PySpark | Distributed data processing |
| Delta Lake | Storage and table management |
| Spark SQL | Data validation and analysis |
| Python | Transformation and automation |

## Source Data

### 1. Member Profile CSV Files

Three country-specific files are provided:

- `IND.csv`
- `USA.csv`
- `AUS.csv`

The source files contain different column names and date formats.

The pipeline standardizes these differences into a common member schema.

### 2. Mileage Redemption JSON

The redemption feed contains a member identifier, feed date and an array of redemption transactions.

Example:

```json
{
  "member_id": "223457",
  "feed_date": "20240115",
  "redemptions": [
    {
      "txn_id": "RX10091",
      "txn_date": "20240110",
      "partner": "AeroLink",
      "miles_redeemed": 12000,
      "status": "COMPLETED"
    },
    {
      "txn_id": "RX10092",
      "txn_date": "20240113",
      "partner": "SkyPoints",
      "miles_redeemed": 5000,
      "status": "PENDING"
    }
  ]
}
```

## Medallion Architecture Implementation

### Bronze Layer – Raw Ingestion

- Ingested the three country-specific CSV files.
- Preserved source records as raw JSON strings.
- Captured source country, source filename, ingestion timestamp, batch ID and record hash.
- Stored raw data in Delta format.

**Target table:** `bronze_member_feed`

### Silver Layer – Standardization and Data Quality

- Standardized different source column names.
- Converted country-specific date formats into standard date columns.
- Calculated member age from date of birth.
- Identified stale members based on flight activity.
- Validated mandatory fields and date values.
- Separated invalid records into a reject table.

Business rules:

| Rule | Logic |
|---|---|
| Age | Calculated using DOB and current date |
| Stale Member | Y if days since last flight > 90 |
| Missing flight date | Stale status = UNKNOWN |
| Invalid dates | Flagged and rejected |
| Missing mandatory fields | Flagged and rejected |

**Target tables:**
- `silver_member`
- `silver_member_rejects`

### Gold Layer – Business-Ready Data

- Created a consolidated member master.
- Segregated member records by country.
- Applied latest-record selection using `LAST_UPDATED_DATE` within each country-specific source ID.
- Created a country-specific technical member key.

**Target tables:**
- `gold_member_master`
- `gold_member_ind`
- `gold_member_usa`
- `gold_member_aus`

### Redemption Processing

- Defined an explicit nested JSON schema.
- Parsed the redemption feed.
- Used PySpark `explode()` to convert the redemption array into individual transaction records.
- Flattened transaction attributes.
- Validated transaction IDs, dates, partner, status and miles redeemed.
- Identified duplicate transaction IDs.
- Separated invalid transactions.

**Target tables:**
- `gold_redemption`
- `silver_redemption_rejects`

## Data Quality and Testing

Implemented automated checks for:

1. Bronze source record counts.
2. Country-wise ingestion counts.
3. Bronze-to-Silver reconciliation.
4. Mandatory field validation.
5. Invalid date detection.
6. Duplicate member key detection within countries.
7. Age and stale-member calculations.
8. Gold master and country-table reconciliation.
9. Duplicate redemption transaction detection.
10. Total redemption miles reconciliation.

The testing notebook produces a PASS/FAIL summary for selected validation checks.

## Sample Data Observations

The supplied member files contain nine records in total.

Observed source-data issues:

- Different date formats across country files.
- Australia contains an invalid enrollment date: `2021-13-13`.
- Australia contains a member with a missing DOB.
- USA does not provide a DOB column.
- Source IDs are repeated across countries.

The redemption JSON example contains two transactions with a combined total of 17,000 miles.

These are observations and expected results from the supplied sample data, not production-scale performance benchmarks.

## Important Design Consideration – Member Identity

The source specification does not establish whether the country-specific member IDs are globally unique or retained when a member moves between countries.

Therefore, the implementation uses a country-specific technical key based on source country and source member ID.

This prevents accidental merging of unrelated members.

True cross-country member movement and global latest-record-wins processing require a globally unique member identifier or an authoritative member identity mapping.

The redemption feed also contains a member ID that has not been confirmed to match the identifiers in the country CSV files. Therefore, the redemption transactions are stored independently until the identity mapping is established.

## Project Structure

```text
SkyPoints-Data-Engineering/
│
├── notebooks/
│   ├── 01_Source_Data_Profiling
│   ├── 02_Bronze_Ingestion
│   ├── 03_Silver_Transformation
│   ├── 04_Gold_Member_Processing
│   ├── 05_Redemption_Processing
│   └── 06_End_to_End_Data_Quality_Tests
│
├── data/
│   ├── IND.csv
│   ├── USA.csv
│   ├── AUS.csv
│   └── redemption.json
│
├── documentation/
│   └── technical_walkthrough.md
│
└── README.md
```

## Execution Order

1. Profile the source files.
2. Run Bronze ingestion.
3. Execute Silver transformations.
4. Execute Gold member processing.
5. Process the redemption JSON feed.
6. Run end-to-end data-quality tests.

Update the notebook paths and catalog configuration to match your Databricks workspace before execution.

## Future Enhancements

- Implement incremental ingestion with batch control.
- Use Delta MERGE for incremental updates.
- Introduce a globally unique member identifier or identity cross-reference table.
- Integrate redemption transactions with the member master after identity resolution.
- Add pipeline orchestration and failure monitoring.
- Extend automated data-quality testing and reconciliation reporting.

## Conclusion

This project demonstrates an end-to-end data engineering workflow involving heterogeneous source data, schema standardization, nested JSON processing, Delta Lake storage, business-rule implementation and automated data-quality validation using PySpark and SQL.