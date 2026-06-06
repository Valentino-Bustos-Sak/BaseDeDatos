import os
import sys
import urllib.parse
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from neo4j import GraphDatabase

# ============================================================================
# 📁 CONFIGURACIÓN DE RUTAS E IMPORTS MODULARES
# ============================================================================
ruta_raiz = "/workspaces/BaseDeDatos"
if ruta_raiz not in sys.path:
    sys.path.append(ruta_raiz)

from funciones.obtener_geografia_offline import obtener_geografia_offline

# Carga de variables de entorno (.env)
ruta_env = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ruta_env)

# ============================================================================
# 🔌 CONEXIONES A LAS BASES DE DATOS
# ============================================================================
PASS_DWH_TEXTO = os.getenv("PASS_DWH")
PASS_SOURCE_TEXTO = os.getenv("PASS_SOURCE")
PASS_MONGO_BASE = os.getenv("PASS_MONGO")
password_neo = os.getenv("NEO4J_PASSWORD")

pass_dwh_segura = urllib.parse.quote_plus(PASS_DWH_TEXTO)
pass_source_segura = urllib.parse.quote_plus(PASS_SOURCE_TEXTO)
pass_mongo_segura = urllib.parse.quote_plus(PASS_MONGO_BASE)

URL_DATAWAREHOUSE = f"postgresql://postgres.gjpqfmnbabmjohobaxfv:{pass_dwh_segura}@aws-1-us-east-1.pooler.supabase.com:6543/postgres?options=-c%20project=gjpqfmnbabmjohobaxfv"
URL_SOURCE = f"postgresql://postgres.nksiwgmgejhbkyyftnub:{pass_source_segura}@aws-1-us-east-2.pooler.supabase.com:6543/postgres?options=-c%20project=nksiwgmgejhbkyyftnub"

engine_source = create_engine(URL_SOURCE)
engine_dwh = create_engine(URL_DATAWAREHOUSE)

def etl_creacion():
    # ------------------------------------------------------------------------
    # 📥 1. EXTRACCIÓN (Extract)
    # ------------------------------------------------------------------------
    print("📥 Extrayendo datos de las fuentes...")
    querys = {
        "usuario": "SELECT * FROM usuario",
        "geografia": "SELECT * FROM geografia", # Ajustado al nombre real si aplica
        "metodo_facturacion": "SELECT * FROM metodo_facturacion",
        "factura": "SELECT * FROM factura"
    }
    
    with engine_source.connect() as conn:
        df_usuario = pd.read_sql(text(querys["usuario"]), con=conn)
        df_geografia = pd.read_sql(text(querys["geografia"]), con=conn)
        df_metodo = pd.read_sql(text(querys["metodo_facturacion"]), con=conn)
        df_factura = pd.read_sql(text(querys["factura"]), con=conn)

    # Extracción MongoDB Atlas
    client_mongo = MongoClient(f"mongodb+srv://vbustossak_db_user:{pass_mongo_segura}@basededatos.dpkfoeh.mongodb.net/?appName=BaseDeDatos")  
    db = client_mongo["<Publicacion>"]
    coleccion = db["posts"]
    cursor = coleccion.find({})
    datos = []
    for doc in cursor:
        datos.append({
            "id_publicacion": str(doc["_id"]),
            "id_usuario_autor": doc.get("id_usuario"),
            "descripcion": doc.get("text"),
            "longitud_caracteres": len(doc.get("text", "")) if doc.get("text") else 0,
            "tema": doc.get("category"),
            "tiene_imagen": bool(doc.get("imagen", False)),
            "tiene_video": bool(doc.get("video", False))
        })
    df_mongo = pd.DataFrame(datos)
    client_mongo.close()
    
    # Extracción Neo4j AuraDB
    driver = GraphDatabase.driver("neo4j+s://42a291ac.databases.neo4j.io", auth=("42a291ac", password_neo))
    query_cypher = """
    MATCH (u:Usuario)-[r]->(p:Publicacion)
    RETURN elementId(r) AS id_actividad,
           u.usuario_id AS id_usuario_fk,
           p.publicacion_id AS id_publicacion_fk,
           type(r) AS tipo_actividad,
           r.dispositivo AS dispositivo,
           r.latitud AS latitud,
           r.longitud AS longitud,
           r.fecha AS fecha_actividad
    """
    with driver.session() as session:
        result = session.run(query_cypher)
        registros_crudos = [dict(record) for record in result]
        df_neo = pd.DataFrame(registros_crudos)
    driver.close()
    # ------------------------------------------------------------------------
    # 🔄 2. TRANSFORMACIÓN Y CARGA DE DIMENSIONES (Transform & Load)
    # ------------------------------------------------------------------------
    with engine_dwh.connect() as conn_dwh:
        print("⚙️ Procesando y cargando dimensiones estáticas...")
        # Dimensiones básicas
        df_usuario.to_sql("usuario", con=conn_dwh, if_exists="append", index=False)
        df_metodo.to_sql("metodo_facturacion", con=conn_dwh, if_exists="append", index=False)

        # Dimensión Geografía (Unificación Relacional + Grafos)
        print("🌍 Geocodificando e integrando catálogo de geografía...")
        df_neo[['pais', 'region', 'ciudad', 'codigo_iso']] = [
            obtener_geografia_offline(lat, lon) for lat, lon in zip(df_neo['latitud'], df_neo['longitud'])
        ]
        df_geo_neo4j_final = df_neo[['pais', 'region', 'ciudad', 'codigo_iso']].drop_duplicates().copy()
        
        df_geografia_unificada = pd.concat([df_geografia, df_geo_neo4j_final], ignore_index=True)
        df_geografia_unificada.drop_duplicates(subset=['pais', 'region', 'ciudad', 'codigo_iso'], inplace=True)
        
        if 'id_geografia' in df_geografia_unificada.columns:
            df_geografia_unificada.drop(columns=['id_geografia'], inplace=True)
        
        # Generamos Clave Subrogada incremental que arranca de 1 de 1
        df_geografia_unificada.insert(0, 'id_geografia', range(1, len(df_geografia_unificada) + 1))
        df_geografia_unificada.to_sql("geografia", con=conn_dwh, if_exists="append", index=False)

        # Dimensión Tiempo (Clave Subrogada Autoincremental 1 a N)
        print("⏳ Estructurando dimensión tiempo...")
        fechas_facturas = pd.to_datetime(df_factura["fecha_alta"])
        
        # Desarmamos el objeto rígido de Neo4j antes de parsear con Pandas
        fechas_actividades_limpias = df_neo["fecha_actividad"].apply(
            lambda x: x.to_native() if hasattr(x, "to_native") else x
        )
        fechas_actividades = pd.to_datetime(fechas_actividades_limpias, utc=True)
        
        todas_las_fechas = pd.concat([fechas_facturas, fechas_actividades]).dropna()
        todas_las_fechas = pd.to_datetime(todas_las_fechas, utc=True)
        fechas_truncadas = todas_las_fechas.dt.floor("h").drop_duplicates()    
        
        dim_tiempo = pd.DataFrame({
            "fecha": fechas_truncadas.dt.tz_localize(None)
        }).sort_values(by="fecha").drop_duplicates()
        
        # Insertamos ID secuencial incremental partiendo de 1
        dim_tiempo.insert(0, "id_tiempo", range(1, len(dim_tiempo) + 1))
        dim_tiempo["anio"] = dim_tiempo["fecha"].dt.year
        dim_tiempo["trimestre"] = dim_tiempo["fecha"].dt.quarter
        dim_tiempo["mes"] = dim_tiempo["fecha"].dt.month
        dim_tiempo["dia"] = dim_tiempo["fecha"].dt.day
        dim_tiempo["dia_semana"] = dim_tiempo["fecha"].dt.dayofweek + 1
        dim_tiempo["hora"] = dim_tiempo["fecha"].dt.hour
        
        dim_tiempo.to_sql("tiempo", con=conn_dwh, if_exists="append", index=False)

        # Dimensiones Dinámicas (Dispositivo y Tipo Actividad)
        dim_dispositivo = pd.DataFrame({"tipo": df_neo["dispositivo"].unique()}).dropna()
        dim_dispositivo.insert(0, "id_dispositivo", range(1, len(dim_dispositivo) + 1))
        dim_dispositivo.to_sql("dispositivo", con=conn_dwh, if_exists="append", index=False)

        dim_tipo_actividad = pd.DataFrame({"descripcion": df_neo["tipo_actividad"].str.lower().unique()}).dropna()
        dim_tipo_actividad.insert(0, "id_tipo_actividad", range(1, len(dim_tipo_actividad) + 1))
        dim_tipo_actividad.to_sql("tipo_actividad", con=conn_dwh, if_exists="append", index=False)

        # Dimensión Publicación (MongoDB)
        df_publicacion_final = df_mongo[[
            "id_publicacion", "tema", "longitud_caracteres", 
            "descripcion", "tiene_imagen", "tiene_video"
        ]].drop_duplicates(subset=["id_publicacion"])
        df_publicacion_final.to_sql("publicacion", con=conn_dwh, if_exists="append", index=False)

        # ------------------------------------------------------------------------
        # 📊 3. CARGA DE TABLAS DE HECHOS (Fact Tables)
        # ------------------------------------------------------------------------
        # --- Hechos Factura ---
        print("📊 Mapeando e insertando hechos_factura...")
        df_factura["fecha_alta_truncada"] = pd.to_datetime(df_factura["fecha_alta"]).dt.floor("h").dt.tz_localize(None)
        
        # Merge para heredar el id_tiempo secuencial correcto
        df_factura_final = df_factura.merge(dim_tiempo, left_on="fecha_alta_truncada", right_on="fecha", how="left")
        
        hechos_factura = df_factura_final[[
            "id_factura", "id_metodo_fk", "id_geografia_fk", 
            "id_usuario_fk", "id_tiempo", "duracion", "monto"
        ]].rename(columns={"id_tiempo": "id_tiempo_fk"})
        
        hechos_factura.to_sql("factura", con=conn_dwh, if_exists="append", index=False)
        print(f"  ✓ Tabla de Hechos [factura] cargada con éxito ({len(hechos_factura)} filas).")

        # --- Hechos Actividad ---
        print("📊 Mapeando e insertando hechos_actividad...")
        df_neo["fecha_actividad_truncada"] = pd.to_datetime(fechas_actividades_limpias, utc=True).dt.floor("h").dt.tz_localize(None)
        df_neo["tipo_actividad"] = df_neo["tipo_actividad"].str.lower()
        
        # Merges para heredar llaves numéricas del catálogo unificado y dimensiones
        df_actividad_mapeada = df_neo.merge(dim_dispositivo, left_on="dispositivo", right_on="tipo", how="left")
        df_actividad_mapeada = df_actividad_mapeada.merge(dim_tipo_actividad, left_on="tipo_actividad", right_on="descripcion", how="left")
        df_actividad_mapeada = df_actividad_mapeada.merge(df_geografia_unificada, on=['pais', 'region', 'ciudad'], how='left')
        df_actividad_mapeada = df_actividad_mapeada.merge(dim_tiempo, left_on="fecha_actividad_truncada", right_on="fecha", how="left")
        print(len(df_actividad_mapeada))
        hechos_actividad = df_actividad_mapeada[[
            "id_actividad",
            "id_usuario_fk",
            "id_publicacion_fk",
            "id_tiempo",
            "id_geografia",
            "id_dispositivo",       
            "id_tipo_actividad"     
        ]].rename(columns={
            "id_tiempo": "id_tiempo_fk",
            "id_geografia": "id_geografia_fk",
            "id_dispositivo": "id_dispositivo_fk", 
            "id_tipo_actividad": "id_tipo_actividad_fk"
        })


        
        hechos_actividad.to_sql("actividad", con=conn_dwh, if_exists="append", index=False)
        print(f"  🚀 Tabla de Hechos [actividad] cargada con éxito ({len(hechos_actividad)} filas).")

    print("\n✅ [ETL COMPLETE] ¡Todo el modelo de datos consolidado en Supabase!")

if __name__ == "__main__":
    etl_creacion()