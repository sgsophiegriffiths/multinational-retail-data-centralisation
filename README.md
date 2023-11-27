# multinational-retail-data-centralisation


This repository contains the code and documentation for a system designed to centralize the sales data of our multinational company. The aim is to create a robust and accessible database that serves as the single source of truth for all sales-related information. By consolidating data from various sources into one centralized location, we aim to enhance accessibility, analysis, and decision-making processes within the organization.


Installation instructions <br />
Before setting up the system, ensure that the following prerequisites are met: <br />
Python (version 3.x) <br />
Database system (e.g., MySQL, PostgreSQL) installed and configured <br />


1.Clone this repository to your local machine: <br />
git clone https://github.com/sgsophiegriffiths/multinational-retail-data-centralisation.git

2.Install the requirements <br />
pip install pandas re boto3 stringI0 requests tabula sqlalchemy yaml

Usage instructions <br />
To extract data: Use methods from the DataExtractor class in data_extraction.py <br />
For data cleaning: Use methods from the DataCleaning class in data_cleaning.py <br />
To upload data to the database: Use methods from the DatabaseConnector class in database_utils.py <br />


File structure of the project <br />

