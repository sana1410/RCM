# 🏥 RCM Analytics Dashboard: Claims + EMR + CPT Insights

This project analyzes healthcare revenue cycle data using **Claims**, **EMR**, and **CPT code datasets**, applying the **Medallion Architecture** in **SQL Server** for scalable data modeling, and delivering actionable insights through an interactive **Power BI dashboard**.

---

## 📌 Project Overview

The goal of this project is to identify inefficiencies in the revenue cycle, such as claim denials, payment delays, and high-risk procedures, by integrating and analyzing:

- Electronic Medical Records (EMR)
- Claims submission and payment data
- CPT (Current Procedural Terminology) codes

---

## 🗂️ Data Sources

| Dataset | Description |
|---------|-------------|
| **EMR Data** | Contains patient visits, diagnosis codes, and provider details |
| **Claims Data** | Includes claim ID, dates, payer, billed amount, paid amount, denial reasons |
| **CPT Code Reference** | Provides metadata on procedure codes (description, category, cost) |

---

## 🏗️ Architecture Used: Medallion (Bronze → Silver → Gold)

**Database**: Microsoft SQL Server

- **Bronze Layer**  
  Raw ingestion from CSV/Excel into staging tables

- **Silver Layer**  
  Cleaned and transformed data (joins between EMR and Claims; date formatting, null handling, CPT enrichment)

- **Gold Layer**  
  Aggregated, analytics-ready views for dashboard consumption  
  Includes KPIs like denial rate, days in AR, and clean claim rate

---

## 📊 Dashboard: Power BI

The Power BI report includes the following key metrics and visuals:

### ✅ KPIs
- Total Claims Submitted
- Total Amount Billed vs. Paid
- Denial Rate
- Average Days in A/R
- Patient Responsibility Rate

### 📈 Visuals
- Line chart: Monthly Denials Over Time
- Bar chart: Denials by CPT Code
- Pie chart: Payer Mix (Medicare, Medicaid, Private)
- Stacked column: AR Aging Buckets
- Table: Top Denied Procedures with Reason

---

## 💡 Business Value

- Identified top denial reasons (e.g., authorization issues, missing documentation)
- Benchmarked provider performance on clean claim rate and A/R collection
- Recommended procedural improvements based on CPT-level denial trends

---

## ⚙️ Technologies Used

- **SQL Server** – Data modeling and ETL using Medallion architecture
- **Power BI** – Visualization and dashboard delivery
- **Python (optional)** – Data exploration and preprocessing
- **Excel/CSV** – Source files for EMR and claims

---

## 🚀 How to Run

1. Load CSV datasets into staging (bronze) tables in SQL Server.
2. Use provided SQL scripts to clean and model data into silver/gold layers.
3. Open `RCM_Dashboard.pbix` in Power BI Desktop and connect to the SQL Server gold layer.
4. Refresh the dashboard to view updated metrics.

---

## 📁 Project Structure


