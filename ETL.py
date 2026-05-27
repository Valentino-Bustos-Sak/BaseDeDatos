import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from neo4j import GraphDatabase
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONEXIONES (ETAPA: EXTRACT)
# ==========================================

# Conexión a MongoDB (Contenido de publicaciones)
mongo_client = MongoClient("mongodb+srv://basededatos.dpkfoeh.mongodb.net/?...", username="vbustossak_db_user", password="TU_PASSWORD")
mongo_db = mongo_client["mi_red_social"]

# Conexión a Neo4j (Grafo de Interacciones)
neo4j_driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "xilqil50xbAWE1kZ"))

# Conexión a Supabase (SQL Transaccional y Data Warehouse)
# Nota: Supabase usa PostgreSQL por detrás.
engine_supabase_tx = create_engine("postgresql://postgres:password@db.supabase.co:5432/postgres")
engine_supabase_dwh = create_engine("postgresql://postgres:password@db.supabase.co:5432/data_warehouse")

def extract_data():
    print("--- Extrayendo datos de las fuentes ---")
    
    # A. Extraer de MongoDB (Publicaciones)
    mongo_posts = list(mongo_db.posts.find({}, {"_id": 1, "image_url": 1, "caption": 1}))
    df_mongo = pd.DataFrame(mongo_posts)
    df_mongo.rename(columns={"_id": str}, inplace=True) # Mongo usa ObjectId o strings
    
    # B. Extraer de Supabase SQL Transaccional (Usuarios, Suscripciones, Geografía)
    # Suponiendo que la tabla usuarios ya tiene país/ciudad y datos de pago
    query_sql = """
        SELECT u.id AS usuario_id, u.username, u.email, u.plan_suscripcion, u.precio_pagado,
               g.pais, g.ciudad, g.id AS geografia_id
        FROM usuarios u
        JOIN geografia g ON u.geografia_id = g.id
    """
    df_sql_users = pd.read_sql(query_sql, con=engine_supabase_tx)
    
    # C. Extraer de Neo4j (Interacciones y Dispositivo)
    # Traemos las relaciones, el tipo de interacción y las propiedades (como dispositivo)
    query_neo4j = """
        MATCH (u:Usuario)-[r]->(p:Publicacion)
        RETURN u.id AS usuario_id, 
               p.id AS publicacion_id, 
               type(r) AS tipo_interaccion, 
               r.dispositivo AS dispositivo,
               r.fecha AS fecha_interaccion
    """
    with neo4j_driver.session() as session:
        result = session.run(query_neo4j)
        df_neo = pd.DataFrame([record.data() for record in result])
        
    return df_mongo, df_sql_users, df_neo

# ==========================================
# 2. TRANSFORMACIÓN (ETAPA: TRANSFORM)
# ==========================================

def transform_data(df_mongo, df_sql_users, df_neo):
    print("--- Transformando datos al Modelo Estrella ---")
    
    # Asegurar fechas en formato datetime
    df_neo['fecha_interaccion'] = pd.to_datetime(df_neo['fecha_interaccion'])
    
    # --- DIMENSIÓN: TIEMPO ---
    df_dim_tiempo = pd.DataFrame({
        'fecha': df_neo['fecha_interaccion'].dt.date.unique()
    })
    df_dim_tiempo['tiempo_id'] = range(1, len(df_dim_tiempo) + 1)
    df_dim_tiempo['año'] = pd.to_datetime(df_dim_tiempo['fecha']).dt.year
    df_dim_tiempo['mes'] = pd.to_datetime(df_dim_tiempo['fecha']).dt.month
    df_dim_tiempo['dia'] = pd.to_datetime(df_dim_tiempo['fecha']).dt.day
    df_dim_tiempo['hora'] = pd.to_datetime(df_neo['fecha_interaccion']).dt.hour # opcional si querés más granularidad
    
    # --- DIMENSIÓN: USUARIO ---
    df_dim_usuario = df_sql_users[['usuario_id', 'username', 'email', 'plan_suscripcion', 'precio_pagado']].copy()
    
    # --- DIMENSIÓN: PUBLICACIÓN ---
    df_dim_publicacion = df_mongo.rename(columns={'_id': 'publicacion_id'})
    
    # --- DIMENSIÓN: GEOGRAFÍA ---
    df_dim_geografia = df_sql_users[['geografia_id', 'pais', 'ciudad']].drop_duplicates()
    
    # --- DIMENSIÓN: TIPO INTERACCIÓN y DISPOSITIVO ---
    # Creando IDs únicos para estas dimensiones basadas en categorías
    df_dim_interaccion = pd.DataFrame({'tipo_interaccion': df_neo['tipo_interaccion'].unique()})
    df_dim_interaccion['interaccion_id'] = range(1, len(df_dim_interaccion) + 1)
    
    df_dim_dispositivo = pd.DataFrame({'dispositivo': df_neo['dispositivo'].unique()})
    df_dim_dispositivo['dispositivo_id'] = range(1, len(df_dim_dispositivo) + 1)
    
    # --- TABLA DE HECHOS: INTERACCIONES ---
    # Cruzamos el DataFrame base de Neo4j con las dimensiones para obtener las Claves Foráneas (FK)
    df_hechos = df_neo.merge(df_dim_interaccion, on='tipo_interaccion') \
                      .merge(df_dim_dispositivo, on='dispositivo') \
                      .merge(df_sql_users[['usuario_id', 'geografia_id']], on='usuario_id')
    
    # Mapear la fecha al tiempo_id correspondiente
    df_hechos['fecha_corta'] = df_hechos['fecha_interaccion'].dt.date
    df_hechos = df_hechos.merge(df_dim_tiempo, left_on='fecha_corta', right_on='fecha')
    
    # Seleccionar solo las columnas necesarias para la tabla de hechos
    # Agregamos métricas numéricas como 'cantidad' (siempre 1 por interacción) o el monto monetario asociado al usuario
    df_hechos = df_hechos.merge(df_dim_usuario[['usuario_id', 'precio_pagado']], on='usuario_id')
    df_hechos.rename(columns={'precio_pagado': 'ingreso_suscripcion_asociado'}, inplace=True)
    df_hechos['cantidad_interacciones'] = 1
    
    df_hechos_final = df_hechos[[
        'usuario_id', 'publicacion_id', 'tiempo_id', 'geografia_id', 
        'interaccion_id', 'dispositivo_id', 'cantidad_interacciones', 'ingreso_suscripcion_asociado'
    ]]
    
    return df_dim_usuario, df_dim_publicacion, df_dim_tiempo, df_dim_geografia, df_dim_interaccion, df_dim_dispositivo, df_hechos_final

# ==========================================
# 3. CARGA (ETAPA: LOAD)
# ==========================================

def load_data(dim_u, dim_p, dim_t, dim_g, dim_i, dim_d, hechos):
    print("--- Cargando datos en el Data Warehouse de Supabase ---")
    
    # `if_exists='replace'` recrea las tablas. En producción se suele usar 'append'.
    dim_u.to_sql('dim_usuario', con=engine_supabase_dwh, if_exists='replace', index=False)
    dim_p.to_sql('dim_publicacion', con=engine_supabase_dwh, if_exists='replace', index=False)
    dim_t.to_sql('dim_tiempo', con=engine_supabase_dwh, if_exists='replace', index=False)
    dim_g.to_sql('dim_geografia', con=engine_supabase_dwh, if_exists='replace', index=False)
    dim_i.to_sql('dim_tipo_interaccion', con=engine_supabase_dwh, if_exists='replace', index=False)
    dim_d.to_sql('dim_dispositivo', con=engine_supabase_dwh, if_exists='replace', index=False)
    
    # La tabla de hechos va al final para evitar errores de integridad referencial (claves foráneas)
    hechos.to_sql('hechos_interacciones', con=engine_supabase_dwh, if_exists='replace', index=False)
    print("🚀 ¡ETL finalizado con éxito!")

# ==========================================
# EJECUCIÓN DEL PIPELINE
# ==========================================
if __name__ == "__main__":
    df_mongo, df_sql, df_neo = extract_data()
    
    dim_u, dim_p, dim_t, dim_g, dim_i, dim_d, hechos = transform_data(df_mongo, df_sql, df_neo)
    
    load_data(dim_u, dim_p, dim_t, dim_g, dim_i, dim_d, hechos)
    
    # Cerrar conexiones libres
    mongo_client.close()
    neo4j_driver.close()
