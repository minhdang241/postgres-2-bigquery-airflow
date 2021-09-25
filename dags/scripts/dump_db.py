import argparse
import logging
import os
from typing import Dict

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")

def dump_db(db_info: Dict[str, str], *, table_name: str, file_path: str)
    conn = None
        try:
            logging.debug(DB_PASSWORD)
            conn = psycopg2.connect(
                        database=db_info['DB_NAME'],
                        user=db_info['DB_USER'],
                        password=db_info['DB_PASSWORD'],
                        host=db_info['DB_HOST'],
                        port=db_info['DB_PORT']
                    )
            cur = conn.cursor()

            with open(file_path, "w") as f:
                sql = f'COPY "{table_name}" TO STDOUT WITH CSV HEADER'
                cur.copy_expert(sql, f)
            cur.close()
        except Exception as err:
            logging.error(err)
            logging.debug("Failed to interact with DB")
        finally:
            if conn is not None:
                conn.close()
                logging.info('DB connection closed')



if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_name', type=str,
            default="Schools")
    parser.add_argument('--file_path', type=str,
            default="/Users/minhdang/Desktop/DE/project/scripts/test.csv")
    args = parser.parse_args()

    db_info = dict()
    db_info['DB_USER'] = os.getenv("DB_USER")
    db_info['DB_PASSWORD'] = os.getenv("DB_PASSWORD")
    db_info['DB_HOST'] = os.getenv("DB_HOST")
    db_info['DB_PORT'] = os.getenv("DB_PORT")
    db_info['DB_NAME'] = os.getenv("DB_NAME")

    dump_db(db_info, table_name=args.table_name, file_path=args.file_path)
