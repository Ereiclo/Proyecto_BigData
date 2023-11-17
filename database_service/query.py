from database_service import get_connection


def query(sql):
    conn, cur = get_connection()
    cur.execute(sql)
    # get rows as dictionaries
    # cur.execute(sql)
    # rows = cur.fetchall()
    # columns = [desc[0] for desc in cur.description]
    # rows = [dict(zip(columns, row)) for row in rows]
    rows = cur.fetchall()
    conn.close()

    return rows


def join(categoria, subcategoria):

    return categoria + '|' + subcategoria


def get_category_ids():

    categorias = query(
        "select distinct categoria,subcategoria from compras order by categoria,subcategoria;")

    category_ids = {}
    current_id = 0
    for categoria in categorias:

        joined_name = join(categoria['categoria'], categoria['subcategoria'])
        category_ids[joined_name] = str(current_id + 1)

        current_id += 1

    return category_ids, categorias


def get_categories_count_with_filter(user, filter_):
    return query(f"""
                        select categoria,subcategoria,sum(importelinea) as precio_total from compras 
                        where codcliente=\'{user}\' and {filter_} 
                        group by categoria,subcategoria 
                        order by categoria,subcategoria;""")
