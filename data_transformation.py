import psycopg2
from psycopg2 import sql
import pandas as pd
import sqlalchemy
from sqlalchemy import Engine
from constants import *
from tabulate import tabulate

def create_sqlalchemy_engine(postgres_user=POSTGRES_USER, 
                             postgres_pass=POSTGRES_PASS, 
                             postgres_host=POSTGRES_HOST, 
                             postgres_port=POSTGRES_PORT, 
                             db_name=DB_NAME) -> Engine:
    return sqlalchemy.create_engine(
        f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_NAME}') 

def show_table(table_name, cursor):

    cursor.execute(f'SELECT * FROM {table_name}')

    results = cursor.fetchall()
    table = tabulate(results, headers=[tup[0] for tup in cursor.description], tablefmt="grid")
    print(table)

def reset():

    # create a connection to the default database in Postgres (postgres)
    connection = psycopg2.connect(dbname="postgres", 
                            user=POSTGRES_USER, 
                            password=POSTGRES_PASS, 
                            host=POSTGRES_HOST, 
                            port=POSTGRES_PORT)
    connection.autocommit = True
    cursor = connection.cursor()

    # drop the employee_db
    cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")

def create_db():
    try:

        # create a connection to the default database in Postgres (postgres)
        connection = psycopg2.connect(dbname="postgres", 
                                user=POSTGRES_USER, 
                                password=POSTGRES_PASS, 
                                host=POSTGRES_HOST, 
                                port=POSTGRES_PORT)
        connection.autocommit = True

        cursor = connection.cursor()

        # check if the source database exists already
        cursor.execute(f"SELECT * FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
            print(f"Created database {DB_NAME}")
        else:
            print(f'{DB_NAME} already exists!')

        # close the connection to 'postgres' db
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("An error occurred:", e)


def load_cleaned_employee_dataset(cursor):

    try:

        # create the table for our clean data
        create_table_statement = f'''
        CREATE TABLE IF NOT EXISTS {CLEAN_DATA_TABLE_NAME} (
            employee_id INT PRIMARY KEY,
            name VARCHAR(500) NOT NULL,
            age INT NOT NULL,
            department VARCHAR(500) NOT NULL,
            date_of_joining DATE NOT NULL,
            years_of_experience INT NOT NULL,
            country VARCHAR(500) NOT NULL,
            salary INT NOT NULL,
            performance_rating VARCHAR(500) NOT NULL
        );
        '''

        cursor.execute(create_table_statement)
        print(f"Table {CLEAN_DATA_TABLE_NAME} is created.")

        # load the raw data into the raw data table
        engine = create_sqlalchemy_engine()
        
        df = pd.read_csv(CLEAN_DATA_FILEPATH)

        df.to_sql(CLEAN_DATA_TABLE_NAME, engine, if_exists='append', index=False)
        print("Clean data has been inserted into the clean data table.")

    except psycopg2.Error as e:
        print("An error occurred:", e)

def average_salary_by_department(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.department, AVG(employee_data.salary) AS average_salary
        INTO salary_to_department_analysis
        FROM employee_data
        GROUP BY employee_data.department;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Average Salary By Department -----\n')
        show_table('salary_to_department_analysis', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)

def average_salary_by_yoe(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.years_of_experience, AVG(employee_data.salary) AS average_salary
        INTO salary_to_tenure_analysis
        FROM employee_data
        GROUP BY employee_data.years_of_experience
        ORDER BY employee_data.years_of_experience ASC;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Average Salary By YOE -----\n')
        show_table('salary_to_tenure_analysis', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)

def average_salary_by_performance(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.performance_rating, AVG(employee_data.salary) AS average_salary
        INTO performance_by_salary_analysis
        FROM employee_data
        GROUP BY employee_data.performance_rating;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Average Salary By Performance Rating -----\n')
        show_table('performance_by_salary_analysis', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)

def average_salary_by_country(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.country, AVG(employee_data.salary) AS average_salary
        INTO salary_by_country_analysis
        FROM employee_data
        GROUP BY employee_data.country;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Average Salary By Country -----\n')
        show_table('salary_by_country_analysis', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)

def average_yoe_by_performance(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.performance_rating, AVG(employee_data.years_of_experience) AS average_yoe
        INTO yoe_by_performance_analysis
        FROM employee_data
        GROUP BY employee_data.performance_rating;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Average YOE by Performance Rating -----\n')
        show_table('yoe_by_performance_analysis', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)

def employees_per_department(cursor):
    try: 
        
        # execute the select statement
        select_statement = f'''
        SELECT employee_data.department, COUNT(employee_data.employee_id) AS num_employees
        INTO employees_per_department
        FROM employee_data
        GROUP BY employee_data.department;
        '''
        cursor.execute(select_statement)

        print('\n\n----- Employees Per Department -----\n')
        show_table('employees_per_department', cursor)

    except psycopg2.Error as e:
        print("An error occurred:", e)


if __name__ == '__main__':

    # 0. reset from any previous executions
    reset()
    
    # 1. create the database if it does not already exist
    create_db()

    # 2. create a common cursor to be used to execute all SQL commands
    connection = psycopg2.connect(dbname=DB_NAME, 
                            user=POSTGRES_USER, 
                            password=POSTGRES_PASS, 
                            host=POSTGRES_HOST, 
                            port=POSTGRES_PORT)
    connection.autocommit = True
    cursor = connection.cursor()

    # 3. load the cleaned employee dataset
    load_cleaned_employee_dataset(cursor)

    # 4. calculate avg salary by dept
    average_salary_by_department(cursor)

    # 5. calculate average salary by yoe
    average_salary_by_yoe(cursor)

    # 6. calculate average salary by performance rating
    average_salary_by_performance(cursor)

    # 7. calculate average salary by country
    average_salary_by_country(cursor)

    # 8. calculate average years of experience by performance rating
    average_yoe_by_performance(cursor)

    # 9. calculate number of employees per department
    employees_per_department(cursor)






    




