# Real-Time Transaction Pattern Detection System

This project simulates a real-time transaction processing pipeline to identify impactful customer behavior patterns across multiple banks (merchants) using data streams and Spark Structured Streaming. The goal is to continuously detect and emit actionable insights for each merchant based on customer transaction behavior.

---

## 📂 Dataset Source

Refer source folder. 

- `transactions.csv` – Contains transaction data of customers across multiple banks (merchants).
- `CustomerImportance.csv` – Contains weightage for each customer and transaction type. A higher weightage implies higher value for the merchant.

---

## 🛠 Project Overview

- There are **N banks (merchants)** and **M customers**.
- Each customer may have accounts in **multiple banks**.
- The system detects 3 specific **patterns** and outputs detection results in real-time.

---

## 🔄 Mechanisms

### 🔹 Mechanism X (Ingestor)
- **Frequency**: Every 1 second
- **Task**: Reads next 10,000 transaction entries from `transactions.csv` on Google Drive
- **Output**: Uploads each 10,000-row chunk as a new file in a specified **S3 folder**

### 🔹 Mechanism Y (Streaming Detector)
- **Trigger**: Starts in parallel with Mechanism X
- **Task**: Listens for new transaction files in S3 and processes them using Spark Structured Streaming
- **Output**: Emits detected patterns **50 at a time**, writing each group to a **uniquely named file** in S3

---

## 🧠 Pattern Definitions

### 🔸 Pattern ID: `PatId1` → `UPGRADE`
- A customer is in the **top 1 percentile** for total number of transactions with a merchant
- And has a **bottom 1 percentile weight** from the CustomerImportance data
- **And ** the total transactions for that merchant **exceed 50,000**
- ⇒ Detection: Merchant wants to `UPGRADE` the customer

### 🔸 Pattern ID: `PatId2` → `CHILD`
- A customer has:
  - **Average transaction value < Rs. 23**
  - And has made **at least 80 transactions** with a merchant
- ⇒ Detection: Merchant wants to mark them as `CHILD`

### 🔸 Pattern ID: `PatId3` → `DEI-NEEDED`
- A merchant has:
  - **Number of female customers < male customers**
  - And more than **100 female customers**
- ⇒ Detection: Merchant should take diversity actions: `DEI-NEEDED`

---

## 🧾 Detection Output Format

Each detection result contains:

- `YStartTime (IST)`
- `detectionTime (IST)`
- `patternId`
- `ActionType`
- `customerName` *(assumed to be `customerId` as name is not provided)*
- `merchantId`

> Fields not applicable for a given pattern should be left as empty strings (`""`).

---

## 💡 Notes & Assumptions

- The `customerId` is assumed to represent `customerName` in the final output as no name field is present.
- Customer and merchant IDs are consistent across both datasets.
- Source datasets are clean:
  - No preprocessing required
  - No row-level duplicates
- `CustomerImportance.csv` is small and can be broadcasted during Spark jobs

---

## 📦 Tech Stack

- **PySpark (Structured Streaming)**
- **Apache Kafka** (optional for real streaming)
- **AWS S3**
- **PostgreSQL**
- **Python**
- **Google Drive (input simulation)**

---

## 🚀 How to Run

1. Run **Mechanism X** to push data chunks to S3.
2. Start **Mechanism Y** (Spark Streaming) to detect patterns in real time.
3. Detected results will be written to S3 in `output/` folder, 50 rows per file.

---
