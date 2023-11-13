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

\copy compras from '/home/ereiclo/universidad/2023-2/bigdata/proyecto/recos_supermercados/supermercados_clean.csv' delimiter ',' csv header;

--eliminar productos con cantidad negativa 
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