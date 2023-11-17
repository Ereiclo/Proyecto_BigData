import psycopg2
import psycopg2.extras
import numpy
from env_loader import load_env
import os


load_env()


def get_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database="compras",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )


    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cur