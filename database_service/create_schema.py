import csv
from database_service import get_connection
from definitions.path import DATA_FILE_PATH_CLEAN
from preprocess_data.clean_index import clean
from preprocess_data.split_csv import split_csv
from psycopg2 import sql

from tqdm import tqdm
import os


def create_schema():
    if not os.path.exists(DATA_FILE_PATH_CLEAN):
        clean()

    conn, cur = get_connection()

    cur.execute("""
        drop table if exists compras;

        CREATE TABLE compras(
            Tienda varchar(255),
            CodCliente varchar(255),
            NombreCliente varchar(255),
            Categoria varchar(255),
            Subcategoria varchar(255),
            codigosap varchar(255),
            descripcion text,
            fecha varchar(255),
            Cantidad real,
            UnidadMedida varchar(255),
            PesoNeto real,
            PrecioUnitario real,
            PorcDescuento real,
            ImporteLinea real,
            ImpDescuento real,
            ImporteLineaBs real,
            CostoUnitario real
        );
    """)

    insert_query = sql.SQL("""
        INSERT INTO compras (
            Tienda, CodCliente, NombreCliente, Categoria, Subcategoria, codigosap,
            descripcion, fecha, Cantidad, UnidadMedida, PesoNeto, PrecioUnitario,
            PorcDescuento, ImporteLinea, ImpDescuento, ImporteLineaBs, CostoUnitario
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s::real, %s, %s::real, %s::real, %s::real,
            %s::real, %s::real, %s::real, %s::real
        )
    """)

    print("Insertando compras en la base de datos...")

    # read line for csv and insert into database (skip first header line)
    with open(DATA_FILE_PATH_CLEAN, 'r', encoding="utf-8") as f:

        n_lines = 0
        for _ in f:
            n_lines += 1

        f.seek(0)
        csv_reader = csv.reader(f)
        next(csv_reader)

        progress_bar = tqdm(total=n_lines-1, unit='compras')

        for row in csv_reader:
            row[-1] = 0 if row[-1] == '' else row[-1]
            try:
                cur.execute(insert_query, row)
            except:
                print(progress_bar.n)
                print(row)
                raise

            progress_bar.update(1)

    print("Eliminando datos invalidos...")

    cur.execute("""
        delete from
            compras
        where
            not (
                (
                    (
                        cantidad * preciounitario * porcdescuento = importelinea
                    )
                    or (
                        porcdescuento = 0
                        and cantidad * preciounitario = importelinea
                    )
                )
                and cantidad = floor(cantidad)
            )
            or codcliente='0' or cantidad = 0;

        delete from  compras where pesoneto <= 0 or preciounitario <= 0 or porcdescuento < 0 or  impdescuento < 0 or costounitario < 0 or cantidad < 0 or importelinea
        < 0 or importelineabs < 0;
    """)

    cur.execute(
        """CREATE INDEX idx_compras_fecha_codcliente ON compras (fecha, codcliente);
        CREATE INDEX idx_codigosap ON compras using hash (codigosap);
        CREATE INDEX idx_descripcion ON compras using hash (descripcion);
        CREATE INDEX idx_categoria ON compras using hash (categoria);
        CREATE INDEX idx_subcategoria ON compras using hash (subcategoria);
        CREATE INDEX idx_porcdescuento ON compras using btree (porcdescuento);
        
        """)

    conn.commit()
    conn.close()
