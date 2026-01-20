import sqlite3


def client_db(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    return conn, cur


def create_tables(cur, conn):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Customer (
        CustomerId INTEGER PRIMARY KEY,
        CustomerVAT TEXT,
        CustomerName TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Product (
        ProductId INTEGER PRIMARY KEY,
        CustomerId INTEGER,
        ProductName TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Address (
        AddressId INTEGER PRIMARY KEY AUTOINCREMENT,
        CustomerId INTEGER,
        Address TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Phone (
        PhoneId INTEGER PRIMARY KEY AUTOINCREMENT,
        CustomerId INTEGER,
        PhoneNumber TEXT
    )
    """)

    conn.commit()
    conn.close()

    print("SQLite database and tables created!")


def initialize_database(db_name):
    connection, cursor = client_db(db_name)
    create_tables(cursor, connection)


if __name__ == "__main__":
    # Initialize both databases
    initialize_database("systemA.db")
    initialize_database("systemB.db")
