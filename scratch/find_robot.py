import sqlite3

def find_string_in_db(db_path, search_str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        # Get all columns for this table
        cursor.execute(f"PRAGMA table_info({table});")
        columns = [row[1] for row in cursor.fetchall()]
        
        for column in columns:
            try:
                query = f"SELECT * FROM {table} WHERE CAST({column} AS TEXT) LIKE ?;"
                cursor.execute(query, (f'%{search_str}%',))
                results = cursor.fetchall()
                if results:
                    print(f"Found in Table: {table}, Column: {column}")
                    for row in results:
                        print(f"  Row: {row}")
            except sqlite3.Error as e:
                # Some columns might not be searchable this way
                pass
    
    conn.close()

if __name__ == "__main__":
    find_string_in_db('backend/ambassadors.db', 'robot')
