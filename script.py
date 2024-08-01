from minio import Minio
from minio.error import S3Error
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

db_conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

def is_file_in_db(filename):
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM file WHERE filename = %s", (filename,))
        return cursor.fetchone() is not None

def remove_file_from_db(filename):
    with db_conn.cursor() as cursor:
        cursor.execute("DELETE FROM file WHERE filename = %s", (filename,))
        db_conn.commit()
        print(f"Removed orphan database entry: {filename}")

def clean_orphan_files(bucket_name):
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT filename FROM file")
        db_files = [row[0] for row in cursor.fetchall()]

    for db_file in db_files:
        try:
            minio_client.stat_object(bucket_name, db_file)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                remove_file_from_db(db_file)
            else:
                print(f"Error occurred when checking file in MinIO: {e}")

def watch_bucket(bucket_name):
    while True:
        try:
            clean_orphan_files(bucket_name)
        except S3Error as e:
            print(f"Error occurred: {e}")

if __name__ == "__main__":
    bucket_name = "teste"
    watch_bucket(bucket_name)
