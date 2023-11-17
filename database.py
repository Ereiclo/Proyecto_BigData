
# connect to postgres

import psycopg2
import psycopg2.extras
import numpy


def connect():

    conn = psycopg2.connect(
        host="localhost",
        database="compras",
        user="postgres",
        password="1234",
        port="5432"
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return conn, cur


# query database
def query(sql):
    conn, cur = connect()
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

    return category_ids


base = "fecha >= '2021-07-01' and fecha <= '2021-07-20'"

clientes = list(map(lambda x: x['codcliente'], query(
    f"select  distinct codcliente from compras where {base} order by codcliente;")))


category_ids = get_category_ids()


f = open("utility2_version2.txt", "w")


count = 0


users = []
for cliente in clientes:

    compras = query(f"""
                    select categoria,subcategoria,sum(importelinea) as precio_total from compras 
                    where codcliente=\'{cliente}\' and {base} 
                    group by categoria,subcategoria 
                    order by categoria,subcategoria;""")

    total_scores = 0
    scores_list = []
    scores_raw = []

    if (len(compras) < 5):
        continue

    users.append(cliente)
    for compra in compras:
        joined_name = join(compra['categoria'], compra['subcategoria'])
        current_score = compra['precio_total']
        scores_list.append((category_ids[joined_name], current_score))
        total_scores += current_score
        scores_raw.append(current_score)

    # print(scores_list)

    scores_list_string = ""

    total_scores = total_scores / len(scores_list)
    if max(scores_raw) == min(scores_raw):
        for (category_id, score) in scores_list:
            scores_list_string += category_id + f":{min(scores_raw)} "
    else:
        for (category_id, score) in scores_list:
            scores_list_string += category_id + ":" + \
                str(score - total_scores) + " "

    f.write(f"{cliente} " + scores_list_string[:-1] + "\n")

    print(f'Clientes procesados: {count + 1}/{len(clientes)}')
    count += 1
