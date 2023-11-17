# import missing imports
import os
from tqdm import tqdm
from definitions.path import RECOMENDATIONS_RESULT_PATH, ROOT_DIR
from database_service.query import query, get_category_ids, join, get_categories_count_with_filter
from collaborative_recommendation.generate_close_products import generate_close_products


def create_recomendations(start, end, best_items=10):

    # if not os.path.exists(UTILITY_MATRIXES_PATH):
    # os.makedirs(UTILITY_MATRIXES_PATH)

    base = f"fecha >= '{start}' and fecha <= '{end}'"

    clientes = list(map(lambda x: x['codcliente'], query(
        f"select  distinct codcliente from compras where {base} order by codcliente;")))

    category_ids, categories_raw = get_category_ids()

    total_spend = query(f"""
                            select avg(importelinea) as total  
                            from compras
                            where {base};""")[0]['total']
    global_spent = query(f"""
                         select categoria,subcategoria,sum(importelinea) as total 
                         from compras 
                         where {base}
                         group by categoria,subcategoria
                         order by categoria,subcategoria;""")

    total_spend_by_category = {}

    for category in global_spent:
        total_spend_by_category[category_ids[join(
            category['categoria'], category['subcategoria'])]] = category['total'] - total_spend

    # filename = f"{UTILITY_MATRIXES_PATH}/{start}_{end}.txt"

    # if os.path.exists(filename):
    # print("La matriz de utilidad ya existe.")
    # return filename

    n = best_items
    dictionary_of_close_product = generate_close_products(start, end)
    # dictionary_of_close_product[id] =  close_products (en id)

    if not os.path.exists(RECOMENDATIONS_RESULT_PATH):
        os.makedirs(RECOMENDATIONS_RESULT_PATH)

    # get file name of file_path with os
    filename = f"{start}_{end}.txt"
    print(filename)

    final_path = RECOMENDATIONS_RESULT_PATH + "/" + filename + ".collaborative"

    f = open(final_path, "w", encoding="utf-8")

    print("Creando recomendaciones...")

    progress_bar = tqdm(total=len(clientes), unit='clientes')

    for cliente in clientes:

        compras = get_categories_count_with_filter(cliente, base)

        # get the average precio total
        total_score = 0

        progress_bar.update(1)
        if (len(compras) < 5):
            continue

        for compra in compras:
            total_score += compra['precio_total']

        total_score = total_score / len(compras)

        for compra in compras:
            compra['precio_total'] = compra['precio_total'] - total_score

        purchased_ids = list(map(lambda x: category_ids[join(
            x['categoria'], x['subcategoria'])], compras))

        possible_ids = [str(i) for i in range(len(categories_raw))]
        possible_ids = sorted(list(set(possible_ids) - set(purchased_ids)))

        # print(purchased_ids)
        # print(len(purchased_ids))
        # print(len(possible_ids))
        # print(possible_ids)

        score_dictionary = {}

        for categoria in compras:
            current_category_id = category_ids[join(
                categoria['categoria'], categoria['subcategoria'])]
            close_products = dictionary_of_close_product[current_category_id]

            for close_product in close_products:
                if close_product not in score_dictionary:
                    score_dictionary[close_product] = {"count": 0, "total": 0}

                score_dictionary[close_product]["count"] += 1
                score_dictionary[close_product]["total"] += (
                    0.99*categoria["precio_total"] + 0.01*total_spend_by_category[close_product])

        for key in score_dictionary:
            score_dictionary[key] = score_dictionary[key]["total"] / \
                score_dictionary[key]["count"]

        f.write(f"{cliente}: ")

        predictions = []
        for id_ in possible_ids:
            if id_ in score_dictionary:
                predictions.append((id_, score_dictionary[id_]))

        predictions = list(filter(lambda x: x[1] >= 0, predictions))

        predictions = sorted(predictions, key=lambda x: x[1], reverse=True)

        line = ""
        for id_, estimate in predictions[:n]:
            line += f"{id_},{estimate}|"

        f.write(line[:-1])
        f.write("\n")
    f.close()

    return final_path
