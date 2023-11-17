import os
from preprocess_data.clean_index import clean
from definitions.path import DATA_FILE_PATH_CLEAN
from database_service.query import query
from database_service.create_schema import create_schema
from database_service.check_schema_existence import check_schema_existence
from als_recommendation.create_utility_matrix import create_utility_matrix
from als_recommendation.create_recommendations import recommend
from utils.get_recomendations import get_recomendations
from als_recommendation.get_loss import get_loss
from collaborative_recommendation.generate_close_products import generate_close_products
from collaborative_recommendation.create_recomendations import create_recomendations


if check_schema_existence():
    print("La base de datos ya existe.")
else:
    print("La base de datos no existe. Creando...")
    create_schema()

# file = create_utility_matrix('2021-07-01', '2021-07-10')

# print(f"El loss es {get_loss(file)}")
# path = recommend(file)
# print(path)

get_recomendations('./recomendations/2021-07-01_2021-07-10.txt.als')

# create_recomendations('2021-07-01', '2021-07-10')

# for data in query("select * from compras limit 5;"):
# print(data)
# print("----")


# print(ROOT_DIR)
