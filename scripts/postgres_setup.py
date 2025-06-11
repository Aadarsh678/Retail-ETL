import os
import psycopg2


def run_sql_file(cursor, filepath):
    with open(filepath, "r") as f:
        sql = f.read()
    cursor.execute(sql)


def create_schemas():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    cur = conn.cursor()

    base_sql_path = os.path.join(os.path.dirname(__file__), "..", "sql")

    for region in ["eu", "asia", "us"]:
        sql_path = os.path.join(base_sql_path, region, "schema.sql")
        print(f"Running SQL for region: {region} from {sql_path}")
        run_sql_file(cur, sql_path)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    create_schemas()
