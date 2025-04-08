# 🚗 Traffic Accidents Data Analytics System

📌 **Project Overview**

This project analyzes UK road safety data by building a normalized **OLTP database**, a dimensional **OLAP data warehouse**, and a **NoSQL MongoDB** database. It leverages Python for data extraction, transformation, and loading, and uses SQL and NoSQL queries to uncover insights about accident patterns, vehicle risks, and environmental factors.

---

🚀 **Objectives**

1. Design and implement a normalized OLTP database from extracted CSV data.  
2. Develop a dimensional data warehouse optimized for OLAP operations.  
3. Build a NoSQL MongoDB database for flexible data access and aggregation.  
4. Create analytical queries using joins, aggregations, views, rollups, and cubes.  
5. Visualize trends and relationships in accident data.

---

⚙️ **Tools Used**

- **Programming Language**: Python, T-SQL, MongoDB Query Language  
- **Libraries**:  
  - `pandas` – Data manipulation  
  - `codecs` – CSV decoding  
  - `pyodbc` – SQL Server integration  
  - `pymongo` – MongoDB integration  
- **Databases**:  
  - SQL Server (OLTP and OLAP)  
  - MongoDB (NoSQL)  
- **Data Source**:  
  [UK Road Safety - Accidents and Vehicles (Kaggle)](https://www.kaggle.com/datasets/tsiaras/uk-road-safety-accidents-and-vehicles)  
- **ETL Scripts**: Python-based CSV extraction and transformation  
- **Data Modeling**: 3NF normalization for OLTP, star schema for OLAP  

---

⚙️ **Automation with Stored Procedures**

To streamline database creation and setup, we developed stored procedures to automate key tasks:

- **OLTP Database (SQL Server – `TermProject`)**  
  - `DropTables`: Drops all OLTP tables and constraints  
  - `CreateVehiclesTables`: Creates `VehicleDetails` and `DriverDetails`  
  - `CreateAccidentTables`: Creates accident-related tables and foreign keys  

- **OLAP Data Warehouse (SQL Server – `TermProjectDW`)**  
  - `DropOLAPTables`: Drops all dimensional and fact tables  
  - `CreateOLAPTables`: Creates `DateDimension`, `LocationDimension`, `VehicleDimension`, `DriversDimension`, `RoadConditionDimension`, and `AccidentVehicleFact`  

These stored procedures ensure consistency and reproducibility when setting up databases from scratch.

---

🧰 **Data Pipeline**

- 🔄 `csvExtract.py`: Extracts random samples from Kaggle datasets.  
- 🧱 OLTP: Normalized schema with foreign key constraints and relational integrity.  
- 🧮 OLAP: Star schema with dimensions like date, location, driver, and vehicle.  
- 📂 MongoDB: Flexible collections with indexed fields for fast access.  
- 🛠️ Python scripts handle CSV preprocessing, ETL, and database operations.

---

📊 **Key Insights from the Analysis**

✅ Accidents in **urban areas** increase under **dark and rainy conditions**.  
✅ **Cars and motorcycles** are the most involved vehicle types.  
✅ **Older vehicles** are more likely to be involved in severe accidents.  
✅ High-risk combinations found in **young drivers with motorcycles**.  
✅ Accidents are more common between **2 PM – 8 PM**, especially on weekdays.

---

📁 **Project Files & Resources**

All `.bak` files, extracted datasets, and supporting Excel files can be found here:  
🔗 [Google Drive – Project Resources](https://drive.google.com/drive/folders/1tzss3_cbWHPpRlaEPd_fx6yn-JkdjfaG?usp=sharing)
