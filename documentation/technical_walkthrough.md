# SkyPoints – Technical Assessment Walkthrough

## 1. Project Introduction

I developed an end-to-end data engineering pipeline for an airline loyalty platform using Databricks, PySpark, Delta Lake and SQL.

The platform receives member profile data from three countries in different CSV formats and mileage redemption transactions through a nested JSON feed.

My objective was to build a pipeline that standardizes heterogeneous source data, applies business rules, performs data-quality validations and produces business-ready datasets.

I implemented the pipeline using the Bronze, Silver and Gold Medallion Architecture.

## 2. Architecture Explanation

I divided the pipeline into three primary layers.

### Bronze – Raw Ingestion

In the Bronze layer, I ingested the India, USA and Australia CSV files.

I deliberately read the CSV columns as strings because the source files have different date formats and identifiers that should not be unintentionally converted during ingestion.

I stored the original records as JSON strings in a Delta table along with metadata such as:
- Source country
- Source filename
- Ingestion timestamp
- Batch ID
- Record hash

This allows me to preserve the original source information and trace records back to their input files.

### Silver – Data Standardization

The Silver layer handles schema standardization and data quality.

The country files contain different column names. For example, India uses `Name`, while Australia uses `Member Name`.

I mapped these fields into a common schema.

I also standardized dates using country-specific formats:

- India: `M/d/yyyy`
- USA: `MMddyyyy`
- Australia: `yyyy-MM-dd`

I used safe date parsing so malformed values return NULL instead of stopping the pipeline.

For example, Australia's `2021-13-13` enrollment date is invalid. I preserve the raw value and flag the record for rejection.

I also derived two business columns:

**Age**

Calculated using the member's DOB and the current date.

**Stale Member**

A member is marked stale when the number of days since their last flight is greater than 90.

Missing flight dates are marked as UNKNOWN.

Invalid records are stored separately in a reject table with validation reasons.

### Gold – Business-Ready Data

In the Gold layer, I created a consolidated member master and separate country-specific tables.

I used a PySpark window function with `ROW_NUMBER()` to select the latest record based on `LAST_UPDATED_DATE`.

The ordering is applied within each country-specific source member ID because the supplied source identifiers are not confirmed to be globally unique.

I created a country-specific technical member key to avoid accidental collisions between records from different countries.

## 3. Redemption JSON Processing

The redemption feed contains one member identifier and an array of transactions.

A single JSON document can therefore contain multiple redemption transactions.

I defined an explicit PySpark schema using `StructType`, `StructField` and `ArrayType`.

I then used `explode()` on the `redemptions` array.

This converts each array element into an individual transaction row.

For example, the sample feed contains two transactions:
- RX10091 – 12,000 miles – COMPLETED
- RX10092 – 5,000 miles – PENDING

The flattened table contains one row per transaction.

I then validated mandatory fields, positive miles, valid dates and duplicate transaction IDs.

The two sample transactions total 17,000 miles.

## 4. Data Quality Strategy

I implemented validation at different stages of the pipeline.

### Source-level validations
- Record count checks
- Country-wise record counts
- Null raw-record checks
- Duplicate payload detection

### Silver-level validations
- Mandatory member fields
- Invalid date formats
- Missing or invalid DOB
- Future date checks
- Duplicate member keys within countries

### Gold-level validations
- Country segregation
- Duplicate technical member keys
- Master-to-country table reconciliation

### Redemption validations
- Missing member ID
- Missing transaction ID
- Invalid transaction date
- Missing partner or status
- Invalid miles redeemed
- Duplicate transaction IDs

### End-to-end reconciliation

I use count reconciliation to ensure that the source records are accounted for.

Bronze count should equal Silver valid records plus Silver rejected records.

I also compare the Silver valid record count with the Gold master count, provided no additional records are discarded during Gold processing.

For redemption data, I validate the transaction count and total miles redeemed.

## 5. Important Design Decisions

### Why did I use Delta Lake?

Delta Lake provides reliable table storage and supports operations such as MERGE, schema evolution and transactional writes.

It is suitable for building a structured data pipeline in Databricks.

### Why did I use an explicit JSON schema?

An explicit schema gives better control over data types and avoids relying on automatic schema inference.

It also makes the processing logic predictable.

### Why did I use explode()?

The redemption transactions are stored as an array inside each JSON record.

Explode converts each array element into a separate row, which makes the data suitable for relational analysis.

### Why did I use try_to_timestamp()?

The source contains malformed dates.

Safe parsing allows the pipeline to continue while invalid values are captured for data-quality investigation.

### Why did I use window functions?

Window functions help identify the latest record based on an update timestamp without losing the ability to compare records within a defined partition.

### Why did I create a technical member key?

The source identifiers repeat across countries.

A composite technical key based on country and source ID avoids treating unrelated people as the same member.

## 6. Known Source Limitation

The source specification does not establish whether member IDs are globally unique or whether they remain unchanged when a member moves countries.

I therefore did not assume that the same numeric ID in different country files represents the same person.

To correctly implement cross-country latest-record-wins processing, I would need a globally unique member identifier or an authoritative identity cross-reference.

Similarly, the redemption feed's member ID has not been confirmed to match the member IDs in the country files.

I kept redemption transactions independent until that mapping is confirmed.

This avoids incorrect member-to-transaction relationships.

## 7. Future Improvements

If this were a production implementation, I would improve it by introducing:

1. Incremental ingestion with a batch-control mechanism.
2. Delta MERGE instead of full-table overwrite.
3. A global member identity mapping.
4. Redemption-to-member reconciliation.
5. Pipeline orchestration and monitoring.
6. Centralized data-quality metrics and alerting.
7. Historical tracking for member changes.

## 8. Closing Statement

Through this project, I demonstrated how to build a data pipeline that handles multiple source formats, applies business transformations, processes nested JSON, manages invalid records and validates data across multiple layers.

I also identified source-data limitations and avoided making assumptions that could compromise member identity or data integrity.

The implementation provides a foundation for a reliable airline loyalty data platform using Databricks and PySpark.