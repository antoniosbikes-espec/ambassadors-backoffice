import sqlite3
import traceback
def delete_ambassador(db_path, aid):
    db = sqlite3.connect(db_path)
    db.execute("PRAGMA foreign_keys = ON;")
    try:
        db.execute("DELETE FROM post_views_history WHERE post_id IN (SELECT id FROM posts WHERE profile_id IN (SELECT id FROM profiles WHERE ambassador_id=?))", (aid,))
        db.execute("DELETE FROM posts WHERE profile_id IN (SELECT id FROM profiles WHERE ambassador_id=?)", (aid,))
        db.execute("DELETE FROM contracts WHERE profile_id IN (SELECT id FROM profiles WHERE ambassador_id=?)", (aid,))
        db.execute("DELETE FROM profile_analyses WHERE profile_id IN (SELECT id FROM profiles WHERE ambassador_id=?)", (aid,))
        db.execute("DELETE FROM profiles WHERE ambassador_id=?", (aid,))
        db.execute("DELETE FROM ambassadors WHERE id=?", (aid,))
        db.commit()
        print(f"Deleted {aid} from {db_path} successfully")
    except Exception as e:
        db.rollback()
        print(f"FAILED {aid} in {db_path}: {e}")
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    delete_ambassador("/Users/amparolois/Documents/Ambassadors/db/ambassadors.db", 2)
