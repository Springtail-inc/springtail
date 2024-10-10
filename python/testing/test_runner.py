import psycopg2
import logging
import os
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

def execute_sql(cursor, sql):
    logging.debug(f"Executing SQL: {sql}")
    cursor.execute(sql)
    logging.debug("SQL executed successfully")

def run_test_case(test_file, db_config, results):
    with open(test_file, 'r') as f:
        content = f.read()

    sections = content.split('##')
    sections = {section.strip().split()[0].lower(): section.split('\n', 1)[1] for section in sections if section.strip()}

    with psycopg2.connect(db_config) as conn:
        conn.autocommit = True
        cur = conn.cursor()

        try:
            for section in ['setup', 'test', 'verify', 'cleanup']:
                if section in sections:
                    logging.info(f"Running {section.upper()} for {test_file}")
                    sql_statements = sections[section].strip().split(';')
                    for sql in sql_statements:
                        if sql.strip():
                            execute_sql(cur, sql)
                    
                    if section == 'verify':
                        cur.execute(sections[section])
                        result = cur.fetchall()
                        logging.info(f"Verification result: {result}")
                        
                        # Check for expected results comment
                        expected_comment = sections[section].strip().split('\n')[0]
                        if expected_comment.startswith('-- Expected:'):
                            expected = eval(expected_comment.split(':', 1)[1].strip())
                            if result != expected:
                                raise AssertionError(f"Verification failed for {test_file}: Expected {expected}, got {result}")
                        elif not result and 'DELETE' not in sections['test'].upper():
                            # Only raise an error for empty results if it's not a DELETE test
                            raise AssertionError(f"Verification failed: No results returned for {test_file}")

            results.passed += 1
            logging.info(f"Test case {test_file} PASSED")
        except psycopg2.Error as e:
            results.failed += 1
            error_msg = f"SQL Error in test case {test_file}: {str(e)}"
            results.errors.append(error_msg)
            logging.error(error_msg)
        except AssertionError as e:
            results.failed += 1
            error_msg = str(e)
            results.errors.append(error_msg)
            logging.error(error_msg)
        except Exception as e:
            results.failed += 1
            error_msg = f"Unexpected error in test case {test_file}: {str(e)}"
            results.errors.append(error_msg)
            logging.error(error_msg)
        finally:
            cur.close()

def run_all_tests(test_folder, db_config):
    logging.info(f"Running all test cases from folder: {test_folder}")
    results = TestResult()
    for file in sorted(os.listdir(test_folder)):
        if file.endswith('.sql'):
            logging.info(f"Running test case: {file}")
            run_test_case(os.path.join(test_folder, file), db_config, results)

    # Print summary statistics
    logging.info("\n--- Test Summary ---")
    logging.info(f"Total tests run: {results.passed + results.failed}")
    logging.info(f"Tests passed: {results.passed}")
    logging.info(f"Tests failed: {results.failed}")
    if results.errors:
        logging.info("\nErrors:")
        for error in results.errors:
            logging.info(error)

if __name__ == "__main__":
    test_folder = 'test_cases'  # Folder with test case files
    db_config = 'dbname=springtail user=springtail host=localhost'

    run_all_tests(test_folder, db_config)