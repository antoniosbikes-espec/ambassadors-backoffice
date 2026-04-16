import sqlite3

def dump_all(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        print(f"--- Table: {table} ---")
        cursor.execute(f"SELECT * FROM {table};")
        for row in cursor.fetchall():
            print(row)
    conn.close()

if __name__ == "__main__":
    print("BACKEND DB:")
    dump_all('backend/ambassadors.db')
    print("\nDB DB:")
    dump_all('db/ambassadors.db')
