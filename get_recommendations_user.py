
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

    return category_ids, categorias


category_ids, categorias = get_category_ids()

base = "fecha >= '2021-07-01' and fecha <= '2021-07-20'"

# clientes = list(map(lambda x: x['codcliente'], query(
# f"select  distinct codcliente from compras where {base} order by codcliente;")))


# data = "1000080: 314,0.21294359862804413|769,0.2075749784708023|126,0.18172596395015717|373,0.1526903659105301|940,0.15086989104747772|413,0.13958260416984558|650,0.13490724563598633|322,0.13154254853725433|933,0.1312762200832367|726,0.12475663423538208"

# data = "1000080: 1127,0.22430358827114105|610,0.2018936276435852|237,0.19478683173656464|192,0.18285398185253143|521,0.174401193857193|127,0.16907259821891785|856,0.16544809937477112|592,0.16534088551998138|329,0.1541072130203247|416,0.14768129587173462"

data = "10060846: 494,0.31201836466789246|525,0.2803533673286438|1127,0.2752758264541626|491,0.2612076997756958|463,0.2535920739173889|906,0.24541106820106506|566,0.24401788413524628|874,0.24117033183574677|711,0.23721550405025482|94,0.234828919172287"

userid, recommended_products = data.split(": ")


recommended_products = map(lambda x: x.split(
    ","), recommended_products.split("|"))


compras = query(f"""
                select distinct categoria,subcategoria from compras 
                where codcliente=\'{userid}\' and {base} 
                order by categoria,subcategoria;""")


for data in compras:
    print(data)

print("                 ")

for category_id, _ in recommended_products:

    category = categorias[int(category_id)]['categoria']
    subcategory = categorias[int(category_id)]['subcategoria']

    print(category, subcategory)
    for data in query(
            f"""select codigosap,descripcion, sum(importelinea) as total 
            from compras 
            where 
            {base} and categoria='{category}' 
            and subcategoria='{subcategory}'
            group by codigosap,descripcion 
            order by total desc;""")[:5]:
        print(data)

    print("-----------------")
