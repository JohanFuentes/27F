import psycopg2
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
from datetime import datetime
from math import comb
import json
from shapely.geometry import GeometryCollection, Point
import random


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
    cursor.execute("SELECT count(*) FROM fibra_optica_vertices_pgr")
    resultados = cursor.fetchall()  # Para consultas SELECT
    n = resultados[0][0] #numero de nodos en la red
    combinaciones = comb(n, 2)
    
    gids_excluir = []
    cursor.execute("SELECT gid,probability FROM fibra_optica ORDER BY gid ASC")
    probabilidades = cursor.fetchall()  # Para consultas SELECT

    numero_random = random.random()
    
    for tupla in probabilidades:
        numero_random = random.random()
        if(numero_random<tupla[1]):
            gids_excluir.append(tupla[0])

    query_sin_exclusion = """
    -- Consulta para identificar componentes conexos
    WITH componentes_conexos AS (
        SELECT * FROM pgr_connectedComponents(
            'SELECT gid AS id, source, target, cost FROM fibra_optica'
        )
    )
    SELECT 
        subconsulta.componente AS componente,
        COUNT(*) AS cantidad_vertices,
        ARRAY_AGG(vertice) AS vertices
    FROM (
        SELECT 
            componentes_conexos.component AS componente,
            fibra_optica_vertices_pgr.id AS vertice
        FROM componentes_conexos
        JOIN fibra_optica_vertices_pgr ON componentes_conexos.node = fibra_optica_vertices_pgr.id
    ) AS subconsulta
    GROUP BY subconsulta.componente
    ORDER BY cantidad_vertices ASC;
    """

    query_con_exclusion = f"""
    -- Consulta para identificar componentes conexos
    WITH componentes_conexos AS (
        SELECT * FROM pgr_connectedComponents(
            'SELECT gid AS id, source, target, cost FROM fibra_optica WHERE gid NOT IN ({','.join(map(str, gids_excluir))})'
        )
    )
    SELECT 
        subconsulta.componente AS componente,
        COUNT(*) AS cantidad_vertices,
        ARRAY_AGG(vertice) AS vertices
    FROM (
        SELECT 
            componentes_conexos.component AS componente,
            fibra_optica_vertices_pgr.id AS vertice
        FROM componentes_conexos
        JOIN fibra_optica_vertices_pgr ON componentes_conexos.node = fibra_optica_vertices_pgr.id
    ) AS subconsulta
    GROUP BY subconsulta.componente
    ORDER BY cantidad_vertices ASC;
    """
    if(len(gids_excluir)==0):
        cursor.execute(query_sin_exclusion)
        islas = cursor.fetchall()  # Para consultas SELECT
        # Listas para almacenar los valores
        contadores = []
        vertices = []

        # Iterar sobre la estructura y extraer los elementos
        for tupla in islas:
            contadores.append(tupla[1])
            vertices.append(tupla[2])  # Utiliza extend para agregar los elementos de la lista

        sumaConexiones = 0
        
        for cantidad in contadores:
            sumaConexiones = comb(cantidad, 2) + sumaConexiones

        print("ATTR:",(sumaConexiones/combinaciones)*100,"%")
    else:
        cursor.execute(query_con_exclusion)
        islas = cursor.fetchall()  # Para consultas SELECT
        # Listas para almacenar los valores
        contadores = []
        vertices = []

        # Iterar sobre la estructura y extraer los elementos
        for tupla in islas:
            contadores.append(tupla[1])
            vertices.append(tupla[2])  # Utiliza extend para agregar los elementos de la lista

        sumaConexiones = 0
        
        for cantidad in contadores:
            sumaConexiones = comb(cantidad, 2) + sumaConexiones

        print("ATTR:",(sumaConexiones/combinaciones)*100,"%")        

    cursor.close()
    conn.close()
    
except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)