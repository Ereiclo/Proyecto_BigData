
import numpy as np
from tqdm import tqdm
from database_service.query import get_category_ids, query, join


def get_cos_distance(vector1, vector2):
    return np.dot(vector1, vector2) / (np.linalg.norm(vector1) * np.linalg.norm(vector2))


def create_categories_vector(start, end):
    categories_dictionary, _ = get_category_ids()
    categories = query(f"""
        select distinct categoria,subcategoria from compras where fecha >= '{start}' and fecha <= '{end}' order by categoria,subcategoria;
    """)
    base = f"fecha >= '{start}' and fecha <= '{end}'"

    current_category = categories[0]['categoria']
    category_id = 0

    vector = []

    print("Procesando vectores de categoria")
    progress_bar = tqdm(total=len(categories), unit='categorias')

    for category in categories:
        if category['categoria'] != current_category:
            category_id += 1
            current_category = category['categoria']
        # print(f"""
        #       select min(importelinea),max(importelinea),avg(importelinea) ,count(*)
        #       from compras where
        #       categoria='{category['categoria']}' and subcategoria = '{category['subcategoria']}';""")
        temp = query(f"""
              select min(importelinea),max(importelinea),avg(importelinea) ,count(*)
              from compras where 
              categoria ='{category['categoria']}' and subcategoria = '{category['subcategoria']}' and {base};""")[0]

        temp2 = query(f"""
              select count(*) 
              from compras where 
              categoria ='{category['categoria']}' and subcategoria = '{category['subcategoria']}' and porcdescuento > 0 and {base};
        """)[0]

        vector.append((categories_dictionary[join(category["categoria"], category["subcategoria"])], np.array([category_id, temp['min'],
                      temp['max'], temp['avg'],  temp2['count'] / temp['count']*100])))
        progress_bar.update(1)

    return vector


# get nn close categories
def get_close_categories(vector_id, vector, categories_vector, n=10):
    distances = []
    for id_, category_vector in categories_vector:
        if vector_id == id_:
            distances.append(-10000000000000000000000000000000000000)
            continue
        distances.append(get_cos_distance(vector, category_vector))

    distances = np.array(distances)
    indexes = np.argsort(distances)[-n:]
    indexes = list(reversed(indexes))
    # print(distances[indexes])

    close_categories = []

    for index in indexes:
        close_categories.append(categories_vector[index][0])
    return close_categories


def generate_close_products(start, end):

    vectors = create_categories_vector(start, end)

    print("Generando productos similares...")
    progress_bar = tqdm(total=len(vectors), unit='categorias')

    result = {}

    for category_id, vector in vectors:

        close_categories = get_close_categories(category_id, vector, vectors)
        # print(category_id, close_categories)

        result[category_id] = close_categories

        progress_bar.update(1)

    return result
