import os
import json
from tqdm import tqdm
from database_service.query import get_category_ids, get_categories_count_with_filter, query

from definitions.path import RECOMENDATIONS_RESULT_PATH_JSON


def get_recomendations(file_path):

    _, categorias = get_category_ids()

    filename = os.path.basename(file_path)
    start, end = filename.split('_')
    end = end[:10]

    f = open(file_path, "r", encoding="utf-8")

    n_lines = 0
    for _ in f:
        n_lines += 1

    f.seek(0)

    progress_bar = tqdm(total=n_lines, unit='clientes')

    base = f"fecha >= '{start}' and fecha <= '{end}'"
    results_dict = {}

    for line in f:
        line = line.strip()
        data = line

        userid, recommended_products = data.split(": ")

        recommended_products = map(lambda x: x.split(
            ","), recommended_products.split("|"))

        compras = get_categories_count_with_filter(userid, base)

        results_dict[userid] = {"compras": [], "recomendaciones": []}

        for data in compras:
            results_dict[userid]["compras"].append(
                {"categoria": data["categoria"], "subcategoria": data["subcategoria"], "total_gastado": data["precio_total"]})

        # print("                 ")

        for category_id, _ in recommended_products:

            category = categorias[int(category_id)]['categoria']
            subcategory = categorias[int(category_id)]['subcategoria']

            reccomendation = []

            # print(category, subcategory)
            for data in query(
                    f"""select codigosap,descripcion, sum(importelinea) as total 
                    from compras 
                    where 
                    {base} and categoria='{category}' 
                    and subcategoria='{subcategory}'
                    group by codigosap,descripcion 
                    order by total desc;""")[:2]:
                # print(data)
                reccomendation.append(
                    {"codigosap": data["codigosap"], "descripcion": data["descripcion"], "est_total_gastado": data["total"]})

            results_dict[userid]["recomendaciones"].append(
                {"categoria": category, "subcategoria": subcategory, 'products': reccomendation})
            # print("-----------------")
        progress_bar.update(1)

    if not os.path.exists(RECOMENDATIONS_RESULT_PATH_JSON):
        os.makedirs(RECOMENDATIONS_RESULT_PATH_JSON)
    # save dictionary to a file
    with open(f"{RECOMENDATIONS_RESULT_PATH_JSON}/{filename}.json", 'w', encoding="utf-8") as file:
        json.dump(results_dict, file, indent=4)
    # print(results_dict)


