# import missing imports
from tqdm import tqdm
from definitions.path import RECOMENDATIONS_RESULT_PATH, ROOT_DIR
from database_service.query import query, get_category_ids, join, get_categories_count_with_filter
from collaborative_recommendation.generate_close_products import generate_close_products


def create_recomendations(start, end):

    # if not os.path.exists(UTILITY_MATRIXES_PATH):
    # os.makedirs(UTILITY_MATRIXES_PATH)

    base = f"fecha >= '{start}' and fecha <= '{end}'"

    clientes = list(map(lambda x: x['codcliente'], query(
        f"select  distinct codcliente from compras where {base} order by codcliente;")))

    category_ids, _ = get_category_ids()

    # filename = f"{UTILITY_MATRIXES_PATH}/{start}_{end}.txt"

    # if os.path.exists(filename):
    # print("La matriz de utilidad ya existe.")
    # return filename

    dictionary_of_close_product = generate_close_products(start, end)

    return

    f = open(ROOT_DIR + f"/{start}_{end}.txt", "w")
    print("Creando recomendaciones...")

    progress_bar = tqdm(total=len(clientes), unit='clientes')

    users = []
    for cliente in clientes:

        compras = get_categories_count_with_filter(cliente, base)
        score_dictionary = {}


        for categoria in compras:
            current_category_id = category_ids[join(categoria['categoria'],categoria['subcategoria'])]
            close_products = dictionary_of_close_product[current_category_id]

            for close_product in close_products:
                if close_product not in score_dictionary:
                    score_dictionary[close_product] = 0
                score_dictionary[close_product] += categoria['precio_total']


        progress_bar.update(1)
        return
    # f.close()

    # return filename
