# Vendor Performance Analysis

A complete data analytics project focused on evaluating vendor efficiency and performance using **Python, SQL, Matplotlib/Seaborn, and Power BI**.

## 🚀 Overview

This project analyzes sales, purchase, and vendor datasets to measure how efficiently vendors are performing — where procurement is concentrated, where capital is getting locked in unsold stock, and whether pricing strategy (like bulk buying) actually pays off. The pipeline covers automated data ingestion, logging, EDA, SQL-based business analysis, and an interactive Power BI dashboard.

## 📊 Key Findings

**1. Procurement is heavily concentrated in a small number of vendors**
The top 10 vendors account for **66% of total purchase dollars**, with Diageo North America alone contributing 16% ($5.0B in purchases, $6.8B in sales). This level of concentration is a supply-chain risk worth flagging to stakeholders.

![Top 10 Vendor Purchase Contribution](images/vendor_purchase_contribution_donut.png)

**2. Freight cost scales almost linearly with purchase volume, but price doesn't drive sales**
A correlation heatmap across numerical features showed `TotalPurchaseQuantity` and `FreightCost` correlated at **0.97**, while `PurchasePrice` had only a weak correlation with `TotalSalesDollars` — meaning unit price changes don't meaningfully move revenue, but volume drives freight cost almost 1:1.

![Correlation Heatmap](images/correlation_heatmap.png)

**3. Bulk purchasing measurably reduces unit price**
Vendors were bucketed into small/medium/large order sizes using quantile binning. Unit purchase price dropped substantially for large orders, confirming that bulk-pricing strategy is worth encouraging — assuming a vendor can handle the inventory.

![Bulk Purchase Impact on Unit Price](images/bulk_purchase_boxplot.png)

**4. A specific set of brands are under-marketed relative to their profitability**
Brands with **high profit margin but low total sales** were isolated using a threshold approach (bottom 15% of sales, top 20% of margin) — these are strong candidates for promotional or pricing attention, since they're profitable but underselling.

![High-Margin, Low-Sales Target Brands](images/brand_target_scatter.png)

**5. $2.71M in capital is locked in unsold inventory**
Calculated as `(TotalPurchaseQuantity - TotalSalesQuantity) × PurchasePrice`, aggregated by vendor. Several vendors (e.g. Alisa Carr Beverages, stock turnover ~0.61) show consistently slow-moving stock, flagging where working capital is sitting idle.

## 📝 Key Features

1. Automated ingestion of CSV data into a SQLite database using Python (SQLAlchemy)
2. Logging system to track data ingestion activity and catch failures
3. Exploratory Data Analysis to identify data quality issues (negative/zero profit, infinite margins) and clean them appropriately
4. Merging multi-table data into a single `vendor_sales_summary` analytical view
5. SQL queries answering specific business questions (vendor concentration, inventory turnover, bulk pricing impact)
6. Visualizations in Matplotlib/Seaborn for exploratory analysis
7. An interactive Power BI dashboard for stakeholder-facing reporting

## 🔧 How to Run the Project

1. Clone the repo and install dependencies:
```bash
   pip install pandas numpy matplotlib seaborn sqlalchemy
```
2. Place raw CSV files in the `dataset/` folder
3. Run the ingestion script to load data into SQLite:
```bash
   python ingestion_db.py
```
4. Run `get_vendor_summary.py` to build the merged `vendor_sales_summary` table
5. Open `vendor performance analysis.ipynb` (or `VendorPerformance.ipynb`) to reproduce the EDA and business-question analysis
6. Open the `.pbix` file in `PowerBI Dashboard/` in Power BI Desktop to view the interactive dashboard

## 🧰 Tech Stack

- **Python**: Pandas, NumPy, Matplotlib, Seaborn, SQLAlchemy, Logging
- **SQL** / **SQLite** database
- **Power BI** for dashboarding

## 🛠️ Future Improvements

1. Add ML-based vendor scoring (composite risk/performance score)
2. Add anomaly detection for unusual purchase or pricing patterns
3. Integrate a live database connection instead of static CSV ingestion

## 📁 Repo Structure
