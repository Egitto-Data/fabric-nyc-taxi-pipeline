# 🚕 NYC Taxi End-to-End Data Engineering & Analytics Pipeline

An end-to-end data pipeline built on **Microsoft Fabric** that automates the ingestion, staging, transformation, and reporting of New York City Yellow Taxi trip records.

---

### NYC Taxi Solution Architecture

![NYC Taxi Solution Architecture](images/projimg.png)

## 📌 Project Overview & Problem Statement

### **The Business Challenge**
The NYC Taxi and Limousine Commission (TLC) generates massive monthly volumes of trip record data containing details on pick-ups, drop-offs, fare amounts, and passenger counts. For analytics teams, ingesting and processing large datasets efficiently on a recurring monthly basis presents key challenges:
* Handling incoming monthly files without duplicating data or re-processing historical runs.
* Ensuring corrupt, out-of-range, or outlier date records are filtered out before business reporting.
* Maintaining a lightweight staging environment while preserving a full historical data archive in presentation models.
* Optimizing computational efficiency across ETL toolsets (evaluating Dataflow Gen2 vs. T-SQL Stored Procedures).

### **The Solution**
This project builds an automated, robust Lakehouse-to-Warehouse data pipeline leveraging **Microsoft Fabric**. It establishes a dynamic ingestion engine that ingests monthly Parquet files, cleanses and enriches trip records with geographic lookup data, logs pipeline watermarks to prevent duplicate loads, and serves formatted datasets directly to Power BI reports via Semantic Data Models.

---
## 🏗️ Organizing the Workspace

![Workspace Structure](images/1.PNG)

---

## 🛠️ Key Technical Features

1. **Storage Layer (Fabric Lakehouse):** Acts as the landing zone for raw monthly TLC trip data stored in Parquet format.
2. **Dynamic Data Factory Pipelines:** Uses parameters and variables to dynamically compute file names, dynamic watermarks, and variable execution dates (`pl_stg_processing_nyc_taxi`).
3. **Automated Staging Management:** Automatically clears/deletes staging tables prior to ingestion while appending historical data safely to presentation tables.
4. **Data Transformation & Cleansing:** Blends lookup data and cleanses outliers using **Dataflow Gen2**, with a high-performance **Stored Procedure** alternative.
5. **Incremental Metadata Logging:** Uses a custom `metadata.processing_log` table tracking row counts, pipeline execution dates, and date watermarks.
6. **Reporting & Analytics:** Exposes the presentation warehouse model via a **Power BI Semantic Model**.

---

## 📊 Data Source

The dataset used in this project originates from the **NYC Taxi & Limousine Commission (TLC) Trip Record Data**. 

* 🔗 **Dataset Access:** You can access the official raw trip data for 2026 directly from the [NYC TLC Trip Record Data Webpage](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
* **Format:** Monthly Parquet Files.
* **Scope:** NYC Yellow Taxi (Medallion) Trip Records and Taxi Zone Location Lookup Files.

---
## Overall Pipeline
![EndToEnd](images/2.PNG)

## 🛢️ Core Pipeline Logic & SQL Scripts
### Script Activity: `Latest Processed Data`

Use the following query inside the **Script Activity** to retrieve the most recent pickup timestamp processed for the staging table:

```sql
SELECT TOP 1 
    latest_processed_pickup 
FROM metadata.processing_log 
WHERE table_processed = 'staging_nyctaxi_yellow'
ORDER BY latest_processed_pickup DESC;
```
### v_date

Pipeline expression for **v_date** Set Variable activity

```
@formatDateTime(addToTime(activity('Latest Processed Date').output.resultSets[0].rows[0].latest_processed_pickup, 1, 'Month'), 'yyyy-MM')
```

