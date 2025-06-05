INSTANCE_CONNECTION_NAME='psc-cloud-sql-test-458401:us-central1:cloudsql-postgres3'
DB_USER='postgres'
DB_PASS='changeme'
DB_NAME='postgres'


import os
import pg8000.dbapi # Or psycopg2, if you prefer
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy


def get_cloud_sql_connection():
    """
    Initializes a connection pool for a Cloud SQL PostgreSQL instance using Private Service Connect.
    """
    # Initialize the Cloud SQL Python Connector
    # Specify ip_type=IPTypes.PSC to force connection over Private Service Connect
    connector = Connector(ip_type=IPTypes.PSC)

    # Function to be used by SQLAlchemy to get a connection
    # Use 'pg8000' as the database driver for PostgreSQL
    def getconn():
        conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000", # Specify the PostgreSQL driver
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        )
        return conn

    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=5,       # Adjust pool size as needed
        max_overflow=2,    # Adjust max_overflow as needed
        pool_timeout=30,   # Seconds to wait for a connection
        pool_recycle=1800  # Recycle connections after 30 minutes (30*60 seconds)
    )
    return pool


if __name__ == "__main__":
    try:
      db_pool = get_cloud_sql_connection()
      print("Successfully created Cloud SQL PostgreSQL connection pool using Private Service Connect.")

      # Example: Fetching data
      with db_pool.connect() as conn:
      # PostgreSQL typically uses 'now()' for current timestamp
        result = conn.execute(sqlalchemy.text("SELECT now() AS current_time"))
        row = result.fetchone()
        print(f"Current time from DB: {row.current_time}")
      # Example: Inserting data (uncomment and modify for your table)
      # with db_pool.connect() as conn:
      #     conn.execute(
      #         sqlalchemy.text("INSERT INTO your_pg_table (column1, column2) VALUES (:val1, :val2)"),
      #         {"val1": "pg_data1", "val2": "pg_data2"}
      #     )
      #     conn.commit()
      #     print("Data inserted successfully into PostgreSQL.")
    except Exception as e:
        print(f"Error connecting to Cloud SQL PostgreSQL: {e}")
    finally:
   # Important: Dispose the connection pool properly in long-running applications
        if 'db_pool' in locals() and db_pool:
                db_pool.dispose()
                print("PostgreSQL connection pool disposed.")
