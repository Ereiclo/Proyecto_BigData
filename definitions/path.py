import os
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
DATA_PATH = os.path.join(ROOT_DIR, 'data')
DATA_FILE = "supermercados.csv"
DATA_FILE_CLEAN = "supermercados_clean.csv"
DATA_FILE_PATH = os.path.join(DATA_PATH, DATA_FILE)
DATA_FILE_PATH_CLEAN = os.path.join(DATA_PATH, DATA_FILE_CLEAN)
UTILITY_MATRIXES_PATH = os.path.join(ROOT_DIR, 'utility_matrixes')
RECOMENDATIONS_RESULT_PATH = os.path.join(ROOT_DIR, 'recomendations')
RECOMENDATIONS_RESULT_PATH_JSON = os.path.join(ROOT_DIR, 'recomendations_json')
