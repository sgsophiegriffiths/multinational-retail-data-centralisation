# multinational-retail-data-centralisation


This repository contains the code and documentation for a system designed to centralize the sales data of our multinational company. The aim is to create a robust and accessible database that serves as the single source of truth for all sales-related information. By consolidating data from various sources into one centralized location, we aim to enhance accessibility, analysis, and decision-making processes within the organization.


Installation instructions <br />
Before setting up the system, ensure that the following prerequisites are met: <br />
Python (version 3.x) <br />
Database system (e.g., MySQL, PostgreSQL) installed and configured <br />


1.Clone this repository to your local machine: <br />
git clone https://github.com/sgsophiegriffiths/multinational-retail-data-centralisation.git

2.Install the requirements <br />
pip install pandas re boto3 stringI0 requests tabula sqlalchemy yaml <br />
Also create a database in your database system called sales_data.

Usage instructions <br />
To extract data: Use methods from the DataExtractor class in data_extraction.py. <br />
To clean data: Use methods from the DataCleaning class in data_cleaning.py <br. />
To upload data to the database: Use methods from the DatabaseConnector class in database_utils.py. <br />


File structure of the project <br />
```
├───.gitignore
├───README.md
├───data_cleaning.py
├───data_extraction.py
├───database_utils.py
```

Project Description <br />
In Milestone 1, I set up this github repo. <br />

In Milestone 2, I established the sales_database, developed Python scripts for data extraction and created methods for various sources (CSV, API, S3 bucket) creating pandas dataframes. I cleaned user data, extracted card details from AWS S3, and uploaded them to the database. Additionally, I retrieved and cleaned store and product data, integrating them into the sales_database. <br />

For Milestone 3, I ensured accurate columns and data types in sales_database tables. I added primary keys to dimension tables and established foreign keys in the orders_table, completing a star-based database schema using SQL. <br />

In Milestone 4, I utilized SQL to query the sales_database, addressing crucial business questions. These included store distribution, peak sales months, online sales, store type contribution, highest sales months, staff headcount, top-performing German store type, and sales velocity. <br />

