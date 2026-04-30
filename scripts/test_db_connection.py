import os
import sys

from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

connection = None
cursor = None

try:
    connection = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )

    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    print(f"Connected to database: {DB_NAME}")
    print()

    if tables:
        print(f"Tables ({len(tables)} found):")
        for (table_name,) in tables:
            print(f"  - {table_name}")
    else:
        print("No tables found. Have you run sql/01_schema.sql yet?")

    print()
    print("Connection test passed.")

except Error as e:
    print(f"MySQL error: {e}", file=sys.stderr)
    print(file=sys.stderr)
    print("Things to check:", file=sys.stderr)
    print("  1. MySQL is running and reachable at "
          f"{DB_HOST}:{DB_PORT}", file=sys.stderr)
    print("  2. Credentials in .env are correct "
          f"(DB_USER={DB_USER!r}, DB_NAME={DB_NAME!r})", file=sys.stderr)
    print("  3. The database exists "
          f"(CREATE DATABASE {DB_NAME};)", file=sys.stderr)
    print("  4. The user has been granted access to that database", file=sys.stderr)
    sys.exit(1)

finally:
    if cursor is not None:
        cursor.close()
    if connection is not None and connection.is_connected():
        connection.close()
