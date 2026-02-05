# Energy Intelligence Platform

This project is my attempt to understand how real world energy data moves from raw market systems to something clean, structured, and explainable.

Instead of starting with dashboards or models, I started at the lowest level: raw operational data, legacy formats, and unclear structure. The goal is to build this step by step, the same way a real data platform would be built.

---

## Data Source (AEMO NEM Dispatch)

The data used in this project comes from public dispatch data published by the Australian Energy Market Operator through NEMWeb.

Each dispatch file represents a single 5 minute interval of the Australian National Electricity Market.

The raw files are provided as zipped CSVs and contain multiple logical tables in a legacy format. For this project, the scope is intentionally limited to a single table.

From each dispatch file, only the **DREGION** table is extracted. This table contains regional electricity prices for each NEM region.

Raw files are treated as temporary inputs and are not intended to be stored long term.

---

## What This Data Represents

At its core, this dataset answers a simple question:

**What was the electricity price in each region at a given 5 minute interval?**

Each clean record represents:
- One region
- One 5 minute settlement interval
- The dispatched regional electricity price

Negative prices are expected and reflect real market behavior.

---

## Data Lifecycle (Current State)

The data currently flows through the following stages:

1. Raw dispatch file downloaded manually  
2. Legacy CSV structure parsed  
3. DREGION table extracted  
4. Only required columns selected  
5. Data written to a clean CSV  
6. Clean data converted to Parquet  
7. Parquet data verified using Python  

This creates a clear separation between raw, clean, and curated data.

---

## Metadata and Lineage

Before writing any processing logic, the data contract and lineage were defined explicitly.

- Schema definitions live under `metadata/schemas`
- Lineage documentation lives under `metadata/lineage`

This makes it clear:
- Where the data comes from  
- What the grain of the dataset is  
- Which columns are considered authoritative  
- How the data moves through the system  

This was done intentionally to avoid building logic on assumptions.

---

## What Has Been Achieved So Far

At this stage, the project includes:

- A real public data source  
- Raw data ingestion (manual for now)  
- Explicit schema definition  
- Lineage documentation  
- Processing logic to extract and clean data  
- Column pruning to reduce noise  
- Parquet storage for efficient querying  
- Programmatic data verification via terminal  

This forms a solid and realistic foundation.

---

## Why Parquet Is Used

The clean dataset is stored in Parquet format instead of CSV.

Parquet is not meant to be human readable. It is designed for systems, not text editors.

Using Parquet allows:
- Smaller storage size  
- Faster reads  
- Column based access  
- Realistic analytics workflows  

Inspection is done through code rather than opening the file directly.

---

## What Comes Next (Planned Work)

The next phase focuses on making the pipeline scalable and automated.

Planned steps, in order:

1. **Batch ingestion**  
   Process multiple 5 minute intervals in a single run  

2. **Retention policy**  
   Automatically delete raw ZIP files after processing  

3. **Automated ingestion**  
   Download new dispatch files directly from NEMWeb  

4. **Dockerized processing**  
   Ensure the pipeline runs consistently across environments  

5. **Kubernetes scheduling**  
   Run ingestion on a fixed schedule similar to the real market  

6. **Backend API**  
   Expose curated data through a service layer  

7. **Frontend visualization**  
   Visualize price movement and data flow with lightweight animations  

Each step builds naturally on the previous one without skipping fundamentals.

---

## Project Mindset

This project is not about rushing to predictions or dashboards.

It is about:
- Understanding real data formats  
- Respecting data lifecycle boundaries  
- Making data movement explicit  
- Being able to explain every decision clearly  

The focus is depth and clarity, not speed.
