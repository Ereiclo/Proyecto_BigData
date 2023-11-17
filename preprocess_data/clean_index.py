from definitions.path import DATA_FILE_PATH_CLEAN, DATA_FILE_PATH
from tqdm import tqdm
from preprocess_data.download_data import fetch_data
from preprocess_data.split_csv import split_csv
import os


def clean():
    # check if supermercados.csv exists
    if not os.path.exists(DATA_FILE_PATH):
        print("La data no existe, descargando...")
        fetch_data()

    data = open(DATA_FILE_PATH,
                "r", encoding="utf-8")
    new_file = open(
        DATA_FILE_PATH_CLEAN, "w", encoding="utf-8")

    # get n of lines of data
    n_lines = 0
    for line in data:
        n_lines += 1

    data.seek(0)

    header = data.readline()

    new_file.write(header)

    print("Limpiando data...")

    progress_bar = tqdm(total=n_lines-1, unit='lineas')

    for line in data:

        # print(f"Procesando Linea {i} de {n_lines}")
        progress_bar.update(1)

        line = line.split(",", 1)[1]
        new_line = ""

        index = 0

        elems = split_csv(line)

        for elem in elems:
            # trim elem and add to new_line
            cleaned_elem = elem.strip()
            if index == 5 and cleaned_elem != "":
                try:
                    cleaned_elem = str(int(float(cleaned_elem)))
                except:
                    print(line)
                    raise "Error"

            new_line += cleaned_elem + ","
            index += 1
        new_line = new_line[:-1]

        new_file.write(new_line + "\n")
