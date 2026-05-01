import os

import mysql.connector
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def db_connection():
    """
    Single MySQL connection shared across the entire test session.

    Session scope avoids the overhead of opening and closing a connection
    for each test function — important when the test suite runs hundreds of
    data-quality checks against a ~190k-row dataset.
    """
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )
    yield connection
    connection.close()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """
    Fresh cursor for each test function.

    Function scope ensures that one test's state (e.g., an unconsumed result
    set) cannot bleed into the next test. The cursor is closed automatically
    after each test whether it passes or fails.
    """
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()
