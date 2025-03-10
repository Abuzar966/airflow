ETL Pipeline with Apache Airflow and PostgreSQL

Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline fetches user data from the Random User API, processes it, and stores it in a PostgreSQL database.

Features

Extract: Retrieves 50 random user records from the API.

Transform: Formats and structures the extracted data.

Load: Inserts the transformed data into a PostgreSQL database.

Automated Scheduling: Runs daily using Apache Airflow.

Technologies Used

Apache Airflow

Python

PostgreSQL

Requests Library

psycopg2 (PostgreSQL adapter for Python)

Project Structure

.
├── etl.py    # Main ETL pipeline script
├── README.md # Project documentation

Installation & Setup

Prerequisites

Ensure you have the following installed:

Docker & Docker Compose (if running Airflow in Docker)

Python 3.7+

PostgreSQL database

Steps to Run

Clone the Repository:

git clone <repository_url>
cd <repository_folder>

Install Dependencies:

pip install apache-airflow requests psycopg2

Start Apache Airflow:

airflow standalone

Ensure PostgreSQL is running and update the connection details in etl.py.

Trigger the DAG:

airflow dags trigger random_user_etl_postgres

ETL Workflow

Extract: Fetches user data from the API.

Transform: Processes raw JSON data into a structured format.

Load: Stores transformed data into PostgreSQL.

PostgreSQL Schema

The data is stored in a table called random_users with the following schema:

Column

Data Type

uuid

TEXT (Primary Key)

full_name

TEXT

gender

TEXT

age

INT

email

TEXT (Unique)

city

TEXT

country

TEXT

Future Improvements

Implement data validation checks.

Integrate a data quality monitoring system.

Deploy using Docker Compose with PostgreSQL and Airflow.

License

This project is licensed under the MIT License.

Author

Mohammed Abuzar
