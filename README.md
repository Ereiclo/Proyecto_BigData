
# Participantes

- Eric Bracamonte Otiniano
- Grover Ugarte
- Jorge Nicho

# Descripción

Este proyecto consitió en crear un motor de recomendacion para compras en un supermercado.
Para ello usamos dos métodos: ALS y filtros colaborativos por items. El archivo de ejemplo que muestra como correr la aplicación es **main.ipynb**. Tambien la carpeta plataforma tiene recomendaciones finales para el periodo 2021-07-01 - 2021-07-10. 

# Instrucciones

## **Disclaimer: Este codigo solo esta comprobado en linux (hay problemas con pyspark en windows)**

Para levantar todas las dependencias del proyecto se debe usar el siguiente comando

```bash
source bash.sh
```

En el jupiter recordar elegir como kernel el python del venv creado. Además poner un archivo .env que tenga el siguiente formato (rellenar {} con sus credenciales):

```config
DB_HOST={}
DB_USER={}
DB_PASSWORD={}
DB_PORT={}
```

Tambien debe crear una base de datos con el nombre **compras** en postgres.
