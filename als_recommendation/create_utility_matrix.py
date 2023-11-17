import os
from tqdm import tqdm
from definitions.path import UTILITY_MATRIXES_PATH

from database_service.check_schema_existence import check_schema_existence
from database_service.query import query, get_category_ids, join, get_categories_count_with_filter


def create_utility_matrix(start, end):

    if not os.path.exists(UTILITY_MATRIXES_PATH):
        os.makedirs(UTILITY_MATRIXES_PATH)

    base = f"fecha >= '{start}' and fecha <= '{end}'"

    clientes = list(map(lambda x: x['codcliente'], query(
        f"select  distinct codcliente from compras where {base} order by codcliente;")))

    category_ids, _ = get_category_ids()

    filename = f"{UTILITY_MATRIXES_PATH}/{start}_{end}.txt"

    if os.path.exists(filename):
        print("La matriz de utilidad ya existe.")
        return filename

    f = open(filename, "w")
    print("Creando matriz de utilidad...")

    progress_bar = tqdm(total=len(clientes), unit='clientes')

    users = []
    for cliente in clientes:

        # compras = query(f"""
        #                 select categoria,subcategoria,sum(importelinea) as precio_total from compras
        #                 where codcliente=\'{cliente}\' and {base}
        #                 group by categoria,subcategoria
        #                 order by categoria,subcategoria;""")
        compras = get_categories_count_with_filter(cliente, base)

        total_scores = 0
        scores_list = []
        scores_raw = []

        progress_bar.update(1)
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

        # print(f'Clientes procesados: {count + 1}/{len(clientes)}')
    f.close()

    return filename
