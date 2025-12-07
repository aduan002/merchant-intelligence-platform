# Merchant Intelligence Platform

This project implements a **complete analytics pipeline and BI reporting solution** using the Olist e-commerce dataset. It covers the full data lifecycle from **raw file ingestion and data quality validation to star schema modeling and Power BI dashboards**.



## Objective

The goal of this project is to enable users to:
- Track **revenue, orders, customers, and delivery performance**
- Analyze **customer retention, geographic demand, and cohort behavior**
- Evaluate **seller performance and logistics efficiency**
- Understand **payment behavior and cancellation risk**


## Architecture Overview

**High-Level Flow:**
1. Raw CSV files stored to **MinIO (Object Storage) via Prefect**
2. Ingested into **PostgreSQL raw tables via Prefect**
3. Cleaned and validated using **SQL transformation models**
4. Modeled into a **star schema**
5. Visualized through **Power BI dashboards**

![High-Level Architecture Diagram](/images/architecture_diagram.png)

---

## Tech Stack

- **Storage:** MinIO  
- **Orchestration:** Prefect (Server & Worker)  
- **Database:** PostgreSQL  
- **Transformations:** SQL  
- **Ingestion:** Python  
- **Containerization:** Docker & Docker Compose  
- **Database UI:** pgAdmin  
- **Visualization:** Power BI  

---

## Dataset

- **Source:** [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/)
- The pipeline is built in a **modular & reusable way**, allowing easy adaptation to other transactional datasets.

---

## Repository Structure

```text
data/                   # Raw CSV datasets  
flows/                  # Prefect orchestration & Python ingestion scripts  
infra/                  # Infrastructure & service configuration  
  ├─ minio/             # MinIO storage setup  
  ├─ prefect/           # Prefect server & worker setup  
power-bi/               # Power BI reports  
queries                 # PostgreSQL SQL transformations  
docker-compose.yml      # Full local infrastructure  
prefect.yaml            # Prefect project configuration  
requirements.txt        # Python dependencies  
README.md               # Project documentation  
```

## Data Pipeline

1. **Raw Layer**
- Raw CSV files stored in MinIO using Prefect
- Serves as immutable data source for ingestion

2. **Ingestion Layer**
- Loads raw data into PostgreSQL tables using Prefect

3. **Transformation Layer (Silver)**
- Cleaning and standardization in SQL
- Automated SQL checks validate data before analytics use:
    - Missing event types
    - Incorrect timestamp ordering
    - Duplicate events
    - Orphaned product IDs
    - Missing user IDs
    - Etc.

4. **Analytics Layer (Gold)**
- Star schema optimized for BI (dims and facts)


## Power BI Dashboards
All dashboards are built on the gold star schema layer for consistency and performance.

### 🟦 1. Executive KPI Overview

![Executive KPI Overview](/images/olist_powerbi_1_Sales,%20Customers%20&%20Delivery%20Performance.png)

**Example Insight:**  
The platform generated **15.84M in gross revenue across 99K orders**, with an **average order value of 160.58** and a **93.22% on-time delivery rate**. Revenue peaked in **May** and declined sharply in **September**, suggesting seasonality or potential operational constraints. **Health & Beauty** and **Watches & Gifts** are the top revenue-generating categories. **São Paulo** is the dominant seller city, indicating strong regional concentration of supply.

---

### 🟩 2. Customer Analytics

![Customer Analytics](/images/olist_powerbi_2_Customer%20Behavior%20&%20Retention.png)

**Example Insight:**  
Customer activity is primarily driven by **new customers**, with returning customers forming a noticeably smaller share across all months, indicating a **high acquisition but low repeat purchase rate**. The **orders per customer distribution** is heavily right-skewed, showing that most customers place only **one order**, while a small subset generates multiple repeat purchases. Revenue by state is highly concentrated, with **São Paulo (SP)** generating the majority of revenue, followed by **Rio de Janeiro (RJ)** and **Minas Gerais (MG)**. The **cohort matrix** shows strong initial acquisition in mid-year months (May–August) but rapid drop-off in subsequent periods, highlighting **retention as a key growth opportunity**. The geographic map confirms that customer demand is heavily concentrated in Brazil’s **southeast region**.

---

### 🟧 3. Seller & Delivery Performance

![Seller & Delivery Performance](/images/olist_powerbi_3_Delivery%20&%20Operations%20Performance.png)

**Example Insight:**  
Average delivery times are longest in **Roraima, Amapá, and Amazonas**, exceeding **25–30 days**, indicating significant logistical challenges in northern regions. Despite this, several high-volume seller states such as **São Paulo, Rio de Janeiro, and Paraná** still exhibit **below-average on-time delivery rates**, suggesting that scale alone does not guarantee delivery efficiency. **Cancellation rates peak in Goiás and Minas Gerais**, while southern states such as **Rio Grande do Sul and Pernambuco** show the lowest cancellation levels. The comparison between delivery status and customer reviews reveals a **strong service quality relationship**, where on-time deliveries achieve average review scores above **4**, while late deliveries fall to slightly above **2**, clearly demonstrating the direct impact of logistics performance on customer satisfaction. These patterns highlight the need for **logistics process improvements and more accurate delivery time estimates** to better manage customer expectations and improve overall service quality.

---

### 🟪 4. Payments & Financial Behavior

![Payments & Financial Behavior](/images/olist_powerbi_4_Payment%20&%20Financial%20Behavior.png)

**Example Insight:**  
The platform is heavily dominated by **credit card payments**, which account for approximately **75% of all orders**, followed by **boleto (~20%)**, while **voucher and debit card** usage remain minimal. Payment method preferences remain **stable across all months**, indicating consistent customer behavior over time. **Credit card and debit card payments exhibit the highest cancellation rates**, while **vouchers show the lowest**, suggesting that prepaid payment methods are associated with lower cancellation risk. The **installments vs. average order value** analysis shows a clear **positive relationship**, where higher-value purchases are increasingly split across more installments, confirming that installment plans play a critical role in enabling larger transactions. This highlights a strong opportunity to **expand and optimize installment-based payment options to further enable higher-value purchases**, while simultaneously **strengthening credit risk controls to mitigate the higher cancellation exposure associated with card-based payments**.


### 🟥 Cross-Dashboard Business Summary

Across all analytical domains, the platform shows strong revenue concentration by **region, payment method, and product category**, while **customer retention and logistics performance emerge as the primary constraints on sustainable growth**. Delivery reliability has a direct and measurable impact on both **cancellations and customer satisfaction**, while **installment-based payments act as a key enabler for higher-value transactions**, highlighting clear levers for both **operational optimization and revenue expansion**.


## How to Run Locally


This project is fully containerized and can be run locally using Docker and Docker Compose.

### Clone the Repository
```bash
git clone <your-repo-url>
cd <your-repo-name>
```

### Build and Start the Infrastructure

This will start:
- PostgreSQL
- pgAdmin
- MinIO
- Prefect Server & Worker

```bash
docker-compose build
docker-compose up -d
```

Verify that all containers are running:
```bash
docker ps
```

### Run the Prefect Ingestion & Transformation Flows

Prefect orchestrates the ingestion and transformation process.
1. Access the Prefect UI
```bash
http://localhost:4200
```
2. Trigger the ingestion flows located in the flows/ directory.
3. These flows will:
- Load raw CSV data to MinIO to preserve an immutable data source
- Load raw CSV data from MinIO into PostgreSQL
- Execute the silver (cleaned) SQL models 
- Run data quality validation checks
- Execute the gold (star schema) SQL models

### Verify Data in PostgreSQL
1. Open pgAdmin
```bash
http://localhost:5050/
```
2. Connect to the PostgreSQL service
3. Verify that the following layers exist:
- Raw tables
- Cleaned (silver) tables
- Fact & dimension (gold) tables

### Open the Power BI Report
1. Open the file `power-bi/olist.pbix`
2. Update the PostgreSQL connection if needed:
- Host: `locahost`
- Port: `5432`
3. Refresh the dataset.
4. All dashboards will now load from your local data warehouse.

### Stopping the Project
To stop all services:
```bash
docker-compose down
```