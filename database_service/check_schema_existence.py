
from database_service import get_connection


def check_schema_existence():

    conn, cur = get_connection()
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'compras');")

    result = cur.fetchone()['exists'] 

    conn.close()

    return result
