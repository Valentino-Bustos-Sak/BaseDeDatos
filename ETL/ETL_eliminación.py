import os
import urllib
from anyio import Path
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
from dotenv import load_dotenv
from pathlib import Path
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd


ruta_env = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ruta_env)
PASS_MONGO_BASE = os.getenv("PASS_MONGO")
pass_mongo_segura = urllib.parse.quote_plus(PASS_MONGO_BASE)
password_neo = os.getenv("NEO4J_PASSWORD")
PASS_DWH_TEXTO = os.getenv("PASS_DWH")
pass_dwh_segura = urllib.parse.quote_plus(PASS_DWH_TEXTO)


client_mongo = MongoClient(f"mongodb+srv://vbustossak_db_user:{pass_mongo_segura}@basededatos.dpkfoeh.mongodb.net/?appName=BaseDeDatos")  
db_mongo = client_mongo["<Publicacion>"] 

driver_neo = GraphDatabase.driver("neo4j+s://42a291ac.databases.neo4j.io", auth=("42a291ac", password_neo))

URL_DATAWAREHOUSE = f"postgresql://postgres.gjpqfmnbabmjohobaxfv:{pass_dwh_segura}@aws-1-us-east-1.pooler.supabase.com:6543/postgres?options=-c%20project=gjpqfmnbabmjohobaxfv"
engine_dwh = create_engine(URL_DATAWAREHOUSE)


def etl_eliminacion():
    try:
        publicaciones_mongo = db_mongo["posts"].find({}, {"_id": 1})
        ids_mongo_vigentes = set(str(pub["_id"]) for pub in publicaciones_mongo)
        print(f"Publicaciones mongo:{len(ids_mongo_vigentes)}")
    except Exception as e:
        print(f"Error al conectar o leer de MongoDB: {e}")
        return

    ids_neo_vigentes = set()
    try:
        with driver_neo.session() as session:
            query = "MATCH ()-[r:PUBLICO|LIKEO|REPOSTEO]->() RETURN elementId(r) AS id"
            ids_neo_vigentes = [str(rec["id"]) for rec in session.run(query) if rec["id"]]
        print(f"Publicaciones neo:{len(ids_neo_vigentes)}")
    except Exception as e:
        print(f"Error al leer Neo4j: {e}")
        return
    finally:
        driver_neo.close()


    with engine_dwh.begin() as conn:
        print("Comparando diferencias")

        df_act_dwh = pd.read_sql(text("SELECT id_actividad FROM public.actividad"), con=conn)
        
        df_act_obsoletas = df_act_dwh[~df_act_dwh["id_actividad"].isin(ids_neo_vigentes)]
        
        if not df_act_obsoletas.empty:
            actividades_a_borrar = df_act_obsoletas["id_actividad"].tolist()
            print(f"Actividades huerfanas: {len(actividades_a_borrar)}")
            conn.execute(
                text("DELETE FROM public.actividad WHERE id_actividad = ANY(:ids);"),
                {"ids": actividades_a_borrar}
            )
            print("Actividades obsoletas removidas exitosamente.")
        else:
            print("No se encontraron actividades obsoletas")

        df_pub_dwh = pd.read_sql("SELECT id_publicacion FROM public.publicacion", con=conn)
        
        df_pub_obsoletas = df_pub_dwh[~df_pub_dwh["id_publicacion"].isin(ids_mongo_vigentes)]
        
        if not df_pub_obsoletas.empty:
            publicaciones_a_borrar = df_pub_obsoletas["id_publicacion"].tolist()
            print(f"Publicaciones huerfanas: {len(publicaciones_a_borrar)}")
            conn.execute(
                text("DELETE FROM public.actividad WHERE id_publicacion_fk = ANY(:ids);"),
                {"ids": publicaciones_a_borrar}
            )
            print("Interacciones asociadas a publicaciones obsoletas  removidas")
            conn.execute(
                text("DELETE FROM public.publicacion WHERE id_publicacion = ANY(:ids);"),
                {"ids": publicaciones_a_borrar}
            )
            print("Publicaciones obsoletas removidas exitosamente.")
        else:
            print("No se encontraron publicaciones obsoletas")

    print("Eliminación terminada")


if __name__ == "__main__":
    etl_eliminacion()