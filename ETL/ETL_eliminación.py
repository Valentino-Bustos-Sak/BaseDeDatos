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

# 2. Conexión Neo4j
driver_neo = GraphDatabase.driver("neo4j+s://42a291ac.databases.neo4j.io", auth=("42a291ac", password_neo))

# 3. Conexión Supabase / PostgreSQL DWH
URL_DATAWAREHOUSE = f"postgresql://postgres.gjpqfmnbabmjohobaxfv:{pass_dwh_segura}@aws-1-us-east-1.pooler.supabase.com:6543/postgres?options=-c%20project=gjpqfmnbabmjohobaxfv"
engine_dwh = create_engine(URL_DATAWAREHOUSE)


def etl_eliminacion():
    print("🧹 Iniciando proceso de depuración y sincronización inversa (ETL Eliminación)...")

    print("🍃 Leyendo publicaciones vigentes desde MongoDB...")
    try:
        # Traemos solo el campo _id mapeado como string (o el campo que uses como id_publicacion)
        # Adaptar si tu clave en Mongo se llama 'id_publicacion' en vez de '_id'
        publicaciones_mongo = db_mongo["posts"].find({}, {"_id": 1})
        ids_mongo_vigentes = set(str(pub["_id"]) for pub in publicaciones_mongo)
        print(f"   -> {len(ids_mongo_vigentes)} publicaciones vigentes encontradas en MongoDB.")
    except Exception as e:
        print(f"❌ Error al conectar o leer de MongoDB: {e}")
        return

    # ============================================================================
    # 📐 PASO 2: Extraer IDs vigentes de Neo4j
    # ============================================================================
    print("📐 Leyendo actividades vigentes desde Neo4j...")
    ids_neo_vigentes = set()
    try:
        with driver_neo.session() as session:
            query = "MATCH ()-[r:PUBLICO|LIKEO|REPOSTEO]->() RETURN elementId(r) AS id"
            ids_neo_vigentes = [str(rec["id"]) for rec in session.run(query) if rec["id"]]
        print(f"  📐 Neo4j: {len(ids_neo_vigentes)} actividades vigentes encontradas.")
    except Exception as e:
        print(f"❌ Error al leer Neo4j: {e}")
        return
    finally:
        driver_neo.close()

    # ============================================================================
    # 🏦 PASO 3: Comparación e Impacto en el Data Warehouse (Supabase)
    # ============================================================================
    print("🏦 Conectando al Data Warehouse para evaluar eliminaciones...")
    with engine_dwh.begin() as conn:
        print("  🏦 Analizando discrepancias en el Data Warehouse...")

        # ------------------------------------------------------------------------
        # ❌ CRITERIO A: Eliminar actividades que ya no existen en Neo4j
        # ------------------------------------------------------------------------
        df_act_dwh = pd.read_sql(text("SELECT id_actividad FROM public.actividad"), con=conn)
        
        # Filtramos usando la máscara booleana isin() invertida (~) de Pandas
        df_act_obsoletas = df_act_dwh[~df_act_dwh["id_actividad"].isin(ids_neo_vigentes)]
        
        if not df_act_obsoletas.empty:
            actividades_a_borrar = df_act_obsoletas["id_actividad"].tolist()
            print(f"    ⚠️ Detectadas {len(actividades_a_borrar)} actividades huérfanas en DWH.")
            
            # Ejecutamos la baja masiva usando text() de SQLAlchemy pasándole la lista
            conn.execute(
                text("DELETE FROM public.actividad WHERE id_actividad = ANY(:ids);"),
                {"ids": actividades_a_borrar}
            )
            print("    ✅ Actividades obsoletas removidas exitosamente.")
        else:
            print("    🎉 Tabla 'actividad' alineada con Neo4j (0 bajas).")

        # ------------------------------------------------------------------------
        # ❌ CRITERIO B: Eliminar publicaciones que ya no existen en MongoDB
        # ------------------------------------------------------------------------
        df_pub_dwh = pd.read_sql("SELECT id_publicacion FROM public.publicacion", con=conn)
        
        # Filtramos lo que está en DWH pero ya no está en MongoDB
        df_pub_obsoletas = df_pub_dwh[~df_pub_dwh["id_publicacion"].isin(ids_mongo_vigentes)]
        
        if not df_pub_obsoletas.empty:
            publicaciones_a_borrar = df_pub_obsoletas["id_publicacion"].tolist()
            print(f"    ⚠️ Detectadas {len(publicaciones_a_borrar)} publicaciones eliminadas en origen.")
            
            # 1. Por integridad, borramos primero las interacciones colgadas de estas publicaciones
            conn.execute(
                text("DELETE FROM public.actividad WHERE id_publicacion_fk = ANY(:ids);"),
                {"ids": publicaciones_a_borrar}
            )
            print("    ✅ Interacciones dependientes eliminadas de la tabla de hechos.")
            
            # 2. Borramos las publicaciones físicamente de la dimensión
            conn.execute(
                text("DELETE FROM public.publicacion WHERE id_publicacion = ANY(:ids);"),
                {"ids": publicaciones_a_borrar}
            )
            print("    ✅ Publicaciones obsoletas removidas de la dimensión.")
        else:
            print("    🎉 Tabla 'publicacion' alineada con MongoDB (0 bajas).")

    print("\n✅ [ETL COMPLETE] ¡El Data Warehouse quedó 100% sincronizado y depurado con los orígenes!")


if __name__ == "__main__":
    etl_eliminacion()