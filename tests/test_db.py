import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_insert_update_delete():
    logging.info("Connecting to the PostgreSQL database")
    conn = psycopg2.connect(dbname="springtail", user="springtail", password="springtail")
    cur = conn.cursor()

    try:
        # Insert a record
        logging.info("Inserting record into test_data")
        cur.execute("INSERT INTO test_data (a, b) VALUES (3, 'test_value') RETURNING *;")
        inserted_record = cur.fetchone()
        logging.info(f"Inserted record: {inserted_record}")
        conn.commit()

        # Update the record
        logging.info("Updating record in test_data")
        cur.execute("UPDATE test_data SET b = 'updated_value' WHERE a = 3 RETURNING *;")
        updated_record = cur.fetchone()
        logging.info(f"Updated record: {updated_record}")
        conn.commit()

        # Delete the record
        logging.info("Deleting record from test_data")
        cur.execute("DELETE FROM test_data WHERE a = 3 RETURNING *;")
        deleted_record = cur.fetchone()
        logging.info(f"Deleted record: {deleted_record}")
        conn.commit()

    finally:
        cur.close()
        conn.close()
