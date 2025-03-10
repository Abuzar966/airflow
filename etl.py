import requests
import psycopg2 
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# PostgreSQL Connection Details
DB_HOST = "postgres"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
TABLE_NAME = "random_users"

# Extract: Fetch 50 user records from Random User API
def extract_data():
    api_url = "https://randomuser.me/api/?results=50"
    response = requests.get(api_url)

    if response.status_code == 200:  
        data = response.json()
        return data["results"]
    else:
        raise Exception(f"Failed to fetch data. Status Code: {response.status_code}")

# Transform: Convert raw JSON into structured format
def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')

    transformed_data = [
        {
            "full_name": f"{user['name']['first']} {user['name']['last']}",
            "gender": user["gender"],
            "age": user["dob"]["age"],
            "email": user["email"],
            "city": user["location"]["city"],
            "country": user["location"]["country"],
            "uuid": user["login"]["uuid"]
        }
        for user in extracted_data
    ]
    return transformed_data

# Load: Insert data into PostgreSQL
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    cur = conn.cursor()

    # SQL execution 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS random_users (
            uuid TEXT PRIMARY KEY,
            full_name TEXT,
            gender TEXT,
            age INT,
            email TEXT UNIQUE,
            city TEXT,
            country TEXT
        );
    """)

    # Insert data 
    for user in transformed_data:
        cur.execute("""
            INSERT INTO random_users (uuid, full_name, gender, age, email, city, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (uuid) DO NOTHING;
        """, (user["uuid"], user["full_name"], user["gender"], user["age"], user["email"], user["city"], user["country"]))

    conn.commit()
    cur.close()
    conn.close()

    print(f" Inserted {len(transformed_data)} records into random_users")

# DAG Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 1),
}

# Define DAG
with DAG(
    dag_id='random_user_etl_postgres',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
