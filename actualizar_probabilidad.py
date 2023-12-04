import psycopg2
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
from datetime import datetime
from math import comb, exp
import json
from shapely.geometry import GeometryCollection, Point

#PARAMETROS BDD
conn_params = {
    "dbname": "gis",
    "user": "usuario",
    "password": "usuario",
    "host": "localhost",
    "port": "20000"  # Opcional si es el puerto por defecto
}

try:
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        f.gid AS id_fibra_optica,
        AVG(t.value) AS promedio_valores
    FROM 
        fibra_optica f
    JOIN 
        terremoto t ON ST_Intersects(f.geom, ST_Buffer(t.geom, 0.6))
    GROUP BY 
        f.gid;
    """
    cursor.execute(query)
    intensidad_terremoto = cursor.fetchall()  # Para consultas SELECT
    # Listas para almacenar los valores
    id_fibra = []
    intensidad = []

    # Iterar sobre la estructura y extraer los elementos
    for tupla in intensidad_terremoto:
        id_fibra.append(tupla[0])
        intensidad.append(tupla[1])  # Utiliza extend para agregar los elementos de la lista

    largo = len(id_fibra)

    i = 0
    with open('backup.sql', 'w') as archivo:
        archivo.write(f"SET CLIENT_ENCODING TO UTF8;\n")
        archivo.write(f"SET STANDARD_CONFORMING_STRINGS TO ON;\n")
        archivo.write(f"BEGIN;\n")

        while i < largo:
            a = -8.5
            b = 1
            probabilidad = 1 / (1 + exp(-(a + b * intensidad[i])))
            archivo.write(f"UPDATE fibra_optica SET probability = {probabilidad} WHERE gid = {id_fibra[i]};\n")
            i = i + 1
            
        archivo.write(f"COMMIT;\n")
        archivo.write('ANALYZE "public"."fibra_optica";')
    cursor.close()
    conn.close()

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)