import json
import psycopg2
import datetime
import requests
import psycopg2.extras

def connect_db():
    try:
        return psycopg2.connect(
            dbname="milestone1db",
            user="postgres",
            password="admin",
            host="localhost"
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def cleanStr4SQL(s):
    return s.replace("'", "''")


def fetch_data_from_census(api_url):
    response = requests.get(api_url)
    return response.json()[1:] if response.status_code == 200 else []


def fetch_and_process_census_data():
    conn = connect_db()
    if not conn:
        return

    population_url = "https://api.census.gov/data/2020/acs/acs5?get=NAME,B01003_001E&for=zip%20code%20tabulation%20area:*"
    income_url = "https://api.census.gov/data/2020/acs/acs5/subject?get=NAME,S1903_C03_001E&for=zip%20code%20tabulation%20area:*"

    population_data = fetch_data_from_census(population_url)
    income_data = fetch_data_from_census(income_url)

    zip_population = {row[2]: int(row[1]) for row in population_data}
    zip_income = {row[2]: float(row[1]) for row in income_data if row[1] != "-666666666"}

    combined_data = [(zip_code, zip_population.get(zip_code, 0), zip_income.get(zip_code, 0.0)) for zip_code in set(zip_population) | set(zip_income)]
    insert_data_into_db(conn, combined_data)
    conn.close()



def insert_data_into_db(conn, data):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Zipcodes (
                zip_code VARCHAR(5) PRIMARY KEY,
                population INT,
                avg_income NUMERIC(10, 1)
            );
        """)
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO Zipcodes (zip_code, population, avg_income) VALUES %s
            ON CONFLICT (zip_code) DO UPDATE SET
            population = EXCLUDED.population,
            avg_income = EXCLUDED.avg_income;
        """, data)
        conn.commit()


if __name__ == "__main__":
    conn = connect_db()
    if conn:
        fetch_and_process_census_data()
        conn.close()
    else:
        print("Failed to connect to the database.")