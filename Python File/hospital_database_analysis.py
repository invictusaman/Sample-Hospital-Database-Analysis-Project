# Exercise 1: Connect to your database server and print its version
# Exercise 2: Fetch Hospital and Doctor Information using hospital Id and doctor Id
# Exercise 3: Get the list Of doctors as per the given specialty and salary
# Exercise 4: Get a list of doctors from a given hospital Exercise 
# Exercise 5: Update doctor experience in years
# Exercise 6: Close the connection and verify it.

import sqlite3
import pandas as pd

# Exercise 1: Connect to your database server and print its version
def connect_to_database(db_file):
    """Connects to the SQLite database and prints version and table information."""
    try:
        sqlconnection = sqlite3.connect(db_file)
        print("Database connected successfully")
        version_query = "SELECT sqlite_version();"
        version = pd.read_sql_query(version_query, sqlconnection)
        print("SQLite version:", version.iloc[0, 0])

        cursor = sqlconnection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print("\nTables in the database:")
        for table in tables:
            table_name = table[0]
            print("\n------------------------------------")
            print(f"Table: {table_name}")
            print("------------------------------------")
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            df = pd.DataFrame(columns, columns=['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'])
            print(df[['name', 'type']])

            print("\nPrinting the table contents\n")
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlconnection)
            print(df)
            print("\n")
        return sqlconnection

    except sqlite3.Error as error:
        print(f"Error occurred: {error}")
        return None

# Exercise 2: Fetch Hospital and Doctor Information using hospital Id and doctor Id
def fetch_hospital_doctor_info(sqlconnection):
    """Fetches and prints hospital and doctor information based on user input."""
    try:
        hospital_id = int(input("\nEnter Hospital ID: "))
        query_hospital = "SELECT * FROM Hospital WHERE ID = ?"
        doctor_id = int(input("Enter Doctor ID: "))
        query_doctor = "SELECT * FROM Doctor WHERE DocID = ?"

        df_hospital = pd.read_sql_query(query_hospital, sqlconnection, params=(hospital_id,))
        df_doctor = pd.read_sql_query(query_doctor, sqlconnection, params=(doctor_id,))

        if not df_hospital.empty:
            print(f"\nFetching Hospital information for ID: {hospital_id}")
            print(df_hospital)
        else:
            print(f"No matching records found for Hospital with ID {hospital_id}.")

        if not df_doctor.empty:
            print(f"\nFetching Doctor information for ID: {doctor_id}")
            print(df_doctor)
        else:
            print(f"No matching records found for Doctor with ID {doctor_id}.")

    except ValueError:
        print("Please enter valid integer IDs.")
    except pd.io.sql.DatabaseError as error:
        print(f"Error occurred: {error}")

# Exercise 3: Get the list Of doctors as per the given specialty and salary
def get_doctors_by_speciality_salary(sqlconnection):
    """Gets and prints doctors based on the given speciality and salary."""
    try:
        speciality = input("\nEnter Speciality information: ").lower()
        salary = float(input("Enter Salary information: "))
        query_doctor = "SELECT * FROM Doctor WHERE LOWER(Speciality) = ? and salary = ?"

        df_doctor = pd.read_sql_query(query_doctor, sqlconnection, params=(speciality, salary))

        if not df_doctor.empty:
            print(f"\nFetching Doctor information for speciality `{speciality}` and salary `{salary}`\n")
            print(df_doctor)
        else:
            print(f"\nNo matching records found for Doctor with speciality `{speciality}` and salary `{salary}`.\n")

    except ValueError:
        print("Please enter valid format for speciality and salary.")
    except pd.io.sql.DatabaseError as error:
        print(f"Error occurred: {error}")

# Exercise 4: Get a list of doctors from a given hospital Exercise 
def get_doctors_by_hospital(sqlconnection):
    """Gets and prints a list of doctors from a given hospital ID."""
    try:
        hospital_id = int(input("\nEnter Hospital ID: "))
        query = """
        SELECT D.*
        FROM Doctor D
        JOIN Hospital H ON D.HospitalID = H.ID
        WHERE H.ID = ?
        """

        df = pd.read_sql_query(query, sqlconnection, params=(hospital_id,))

        if not df.empty:
            print(f"\nFetching Doctor information for hospital with id `{hospital_id}`\n")
            print(df)
        else:
            print(f"\nNo matching records found for Doctor.\n")

    except ValueError:
        print("Please enter valid format for ID.")
    except pd.io.sql.DatabaseError as error:
        print(f"Error occurred: {error}")

# Exercise 5: Update doctor experience in years
def update_doctor_experience(sqlconnection):
    """Updates the experience of a doctor based on the given Doctor ID."""
    try:
        cursor = sqlconnection.cursor()
        doctor_id = int(input("\nEnter Doctor ID: "))
        new_experience = input("Enter new experience (in years): ")

        find_doctor = "SELECT * FROM Doctor WHERE DocID = ?"
        cursor.execute(find_doctor, (doctor_id,))
        result = cursor.fetchone()

        if result:
            print("Before update:", result)
            update_query = "UPDATE Doctor SET Exp = ? WHERE DocID = ?"
            cursor.execute(update_query, (new_experience, doctor_id))
            sqlconnection.commit()
            print(f"\nExperience updated for Doctor ID {doctor_id}")
            cursor.execute(find_doctor, (doctor_id,))
            print("After update:", cursor.fetchone())
        else:
            print(f"No doctor found with ID {doctor_id}")

    except ValueError:
        print("Please enter valid format for ID.")
    except sqlite3.Error as error:
        print(f"Error occurred: {error}")

# Exercise 6: Close the connection and verify it.
def close_and_verify_connection(sqlconnection):
    """Closes the database connection and verifies if it's closed."""
    if sqlconnection:
        sqlconnection.close()
        print("\nDatabase connection closed.")

    try:
        sqlconnection.execute("SELECT 1")
        print("The connection is still open.")
    except:
        print("The connection is closed.")

# Main Program
def main():
    connection = connect_to_database("HospitalInfo.db")
    if connection:
        fetch_hospital_doctor_info(connection)
        get_doctors_by_speciality_salary(connection)
        get_doctors_by_hospital(connection)
        update_doctor_experience(connection)
        close_and_verify_connection(connection)

if __name__ == "__main__":
    main()
