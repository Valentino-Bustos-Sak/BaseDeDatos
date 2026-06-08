import os
import sys
import urllib.parse
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from neo4j import GraphDatabase
ruta_raiz = "/workspaces/BaseDeDatos"
if ruta_raiz not in sys.path:
    sys.path.append(ruta_raiz)
from funciones.obtener_geografia_offline import obtener_geografia_offline


ruta_env = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ruta_env)


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
    print("Extrayendo datos de las fuentes")
    # Extracción Supa
    querys = {
        "usuario": "SELECT * FROM usuario",
        "geografia": "SELECT * FROM geografia",
        "metodo_facturacion": "SELECT * FROM metodo_facturacion",
        "factura": "SELECT * FROM factura"
    }
    
    with engine_source.connect() as conn:
        df_usuario = pd.read_sql(text(querys["usuario"]), con=conn)
        df_geografia = pd.read_sql(text(querys["geografia"]), con=conn)
        df_metodo = pd.read_sql(text(querys["metodo_facturacion"]), con=conn)
        df_factura = pd.read_sql(text(querys["factura"]), con=conn)

    # Extracción MongoDB 
    client_mongo = MongoClient(f"mongodb+srv://vbustossak_db_user:{pass_mongo_segura}@basededatos.dpkfoeh.mongodb.net/?appName=BaseDeDatos")  
    db = client_mongo["<Publicacion>"]
    coleccion = db["posts"]
    cursor = coleccion.find({})
    datos = []
    for doc in cursor:
        datos.append({
            "id_publicacion": str(doc["_id"]),
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
    MATCH (u:Usuario)-[r:PUBLICO|LIKEO|REPOSTEO]->(p:Publicacion)
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
        df_neo = pd.DataFrame([record.data() for record in result])
    driver.close()
    
    #Transformación y carga
    with engine_dwh.connect() as conn_dwh:
        print("Procesando y cargando las dimensiones")
        usuarios_viejos = pd.read_sql(text("SELECT id_usuario FROM usuario"), con=conn_dwh)["id_usuario"].tolist()
        df_usuario_nuevo = df_usuario[~df_usuario["id_usuario"].isin(usuarios_viejos)]
        if not df_usuario_nuevo.empty:
            print(f"Se carrgaron {len(df_usuario_nuevo)} usuarios nuevos")
            df_usuario_nuevo.to_sql("usuario", con=conn_dwh, if_exists="append", index=False)       

        metodos_viejos = pd.read_sql(text("SELECT id_metodo_facturacion FROM metodo_facturacion"), con=conn_dwh)["id_metodo_facturacion"].tolist()
        df_metodo_nuevo = df_metodo[~df_metodo["id_metodo_facturacion"].isin(metodos_viejos)]
        if not df_metodo_nuevo.empty:
            df_metodo_nuevo["metodo_pago"] = df_metodo_nuevo["metodo_pago"].astype(str).str.strip().str.lower()
            print(f"Se carrgaron {len(df_metodo_nuevo)} métodos de facturación nuevos")
            df_metodo_nuevo.to_sql("metodo_facturacion", con=conn_dwh, if_exists="append", index=False)

        df_neo[['pais', 'region', 'ciudad', 'codigo_iso']] = [
            obtener_geografia_offline(lat, lon) for lat, lon in zip(df_neo['latitud'], df_neo['longitud'])
        ]
        df_geo_neo4j_final = df_neo[['pais', 'region', 'ciudad', "codigo_iso"]].copy()
        df_geografia_unificada= pd.concat([df_geografia, df_geo_neo4j_final], ignore_index=True)
        for col in ['pais', 'region', 'ciudad']:
            df_geografia_unificada[col] = df_geografia_unificada[col].astype(str).str.strip().str.lower()
        df_geografia_unificada.drop_duplicates(subset=['pais', 'region', 'ciudad'], inplace=True)
        geo_viejas_df = pd.read_sql(text("SELECT id_geografia, pais, region, ciudad, codigo_iso FROM geografia"), con=conn_dwh)
        df_geo_merge = df_geografia_unificada.merge(geo_viejas_df, on=['pais', 'region', 'ciudad'], how='left', suffixes=('', '_old'))
        filas_nuevas_mascara = df_geo_merge['id_geografia_old'].isna()
        
        if filas_nuevas_mascara.any():
            max_id_geo = geo_viejas_df['id_geografia'].max() if not geo_viejas_df.empty else 0
            cantidad_nuevas = filas_nuevas_mascara.sum()
            
            df_geo_merge.loc[filas_nuevas_mascara, 'id_geografia'] = range(
                int(max_id_geo) + 1, 
                int(max_id_geo) + 1 + cantidad_nuevas
            )
        
            df_geo_nuevo = df_geo_merge[filas_nuevas_mascara].copy()
                  
            df_geo_nuevo = df_geo_nuevo[['id_geografia', 'pais', 'region', 'ciudad', 'codigo_iso']]
            print(f"Se cargaron {len(df_geo_nuevo)} geografías nuevas")
            df_geo_nuevo.to_sql("geografia", con=conn_dwh, if_exists="append", index=False)

        
        fechas_facturas = pd.to_datetime(df_factura["fecha_alta"])
        
        fechas_actividades_limpias = df_neo["fecha_actividad"].apply(
            lambda x: x.to_native() if hasattr(x, "to_native") else x
        )

        todas_las_fechas = pd.concat([fechas_facturas, fechas_actividades_limpias]).dropna()
        fechas_facturas = pd.to_datetime(fechas_facturas, utc=True)

        fechas_truncadas = todas_las_fechas.dt.floor("h").drop_duplicates()    
        
        dim_tiempo_candidata = pd.DataFrame({
            "id_tiempo": fechas_truncadas.dt.strftime("%Y%m%d%H").astype(int),
            "fecha": fechas_truncadas.dt.tz_localize(None),
            "anio": fechas_truncadas.dt.year,
            "trimestre": fechas_truncadas.dt.quarter,
            "mes": fechas_truncadas.dt.month,
            "dia": fechas_truncadas.dt.day,
            "dia_semana": fechas_truncadas.dt.dayofweek + 1,
            "hora": fechas_truncadas.dt.hour
        }).drop_duplicates(subset=["id_tiempo"])

        tiempos_viejos = pd.read_sql(text("SELECT id_tiempo FROM tiempo"), con=conn_dwh)["id_tiempo"].tolist()
        dim_tiempo_nueva = dim_tiempo_candidata[~dim_tiempo_candidata["id_tiempo"].isin(tiempos_viejos)]
        if not dim_tiempo_nueva.empty:
            print(f"Se cargaron {len(dim_tiempo_nueva)} tiempos nuevos")
            dim_tiempo_nueva.to_sql("tiempo", con=conn_dwh, if_exists="append", index=False)
    
        dim_dispositivo_viejos = pd.read_sql(text("SELECT tipo FROM dispositivo"), con=conn_dwh)["tipo"].tolist()
        nuevos_dispositivos = [d for d in df_neo["dispositivo"].dropna().unique() if d not in dim_dispositivo_viejos]
        if nuevos_dispositivos:
            max_id_disp = pd.read_sql(text("SELECT COALESCE(MAX(id_dispositivo), 0) as m FROM dispositivo"), con=conn_dwh)['m'].iloc[0]
            dim_dispositivo_nuevo = pd.DataFrame({"tipo": nuevos_dispositivos})
            dim_dispositivo_nuevo.insert(0, "id_dispositivo", range(int(max_id_disp) + 1, int(max_id_disp) + 1 + len(dim_dispositivo_nuevo)))
            print(f"Se cargaron {len(dim_dispositivo_nuevo)} dispositivos nuevos")
            dim_dispositivo_nuevo.to_sql("dispositivo", con=conn_dwh, if_exists="append", index=False)

        
        dim_tipo_viejos = pd.read_sql(text("SELECT descripcion FROM tipo_actividad"), con=conn_dwh)["descripcion"].tolist()
        nuevas_actividades = [a.lower() for a in df_neo["tipo_actividad"].dropna().unique() if a.lower() not in dim_tipo_viejos]
        if nuevas_actividades:
            max_id_tipo = pd.read_sql(text("SELECT COALESCE(MAX(id_tipo_actividad), 0) as m FROM tipo_actividad"), con=conn_dwh)['m'].iloc[0]
            dim_tipo_nuevo = pd.DataFrame({"descripcion": nuevas_actividades})
            dim_tipo_nuevo.insert(0, "id_tipo_actividad", range(int(max_id_tipo) + 1, int(max_id_tipo) + 1 + len(dim_tipo_nuevo)))
            dim_tipo_nuevo.to_sql("tipo_actividad", con=conn_dwh, if_exists="append", index=False)
            print(f"Se cargaron {len(dim_tipo_nuevo)} tipos de actividad nuevos")
            
        publicaciones_viejas = pd.read_sql(text("SELECT id_publicacion FROM publicacion"), con=conn_dwh)["id_publicacion"].tolist()
        df_mongo_nuevo = df_mongo[~df_mongo["id_publicacion"].isin(publicaciones_viejas)].drop_duplicates(subset=["id_publicacion"])
        if not df_mongo_nuevo.empty:
            df_mongo_nuevo.to_sql("publicacion", schema="public", con=conn_dwh, if_exists="append", index=False)
            print(f"Se cargaron {len(df_mongo_nuevo)} publicaciones nuevas")
            
        #Tabla de hechos
        facturas_viejas = pd.read_sql("SELECT id_factura FROM factura", con=conn_dwh)["id_factura"].tolist()
        df_factura_nueva = df_factura[~df_factura["id_factura"].isin(facturas_viejas)].copy()
        
        if not df_factura_nueva.empty:
            df_factura_nueva["id_tiempo_fk"] = pd.to_datetime(df_factura_nueva["fecha_alta"]).dt.strftime("%Y%m%d%H").astype(int)
            hechos_factura = df_factura_nueva[[
                "id_factura", "id_metodo_fk", "id_geografia_fk", 
                "id_usuario_fk", "id_tiempo_fk", "duracion", "monto"
            ]]
            hechos_factura.to_sql("factura", con=conn_dwh, if_exists="append", index=False)
            print(f"Se cargaron {len(hechos_factura)} facturas nuevas")


        dim_tipo_actividad_completa = pd.read_sql(text("SELECT id_tipo_actividad, descripcion FROM tipo_actividad"), con=conn_dwh)
        df_geografia_unificada_completa = pd.read_sql(text("SELECT id_geografia, pais, region, ciudad FROM geografia"), con=conn_dwh)
        dim_dispositivo_completa = pd.read_sql(text("SELECT id_dispositivo, tipo FROM dispositivo"), con=conn_dwh)
        actividades_viejas = pd.read_sql(text("SELECT id_actividad FROM actividad"), con=conn_dwh)["id_actividad"].tolist()

        df_neo_nuevo = df_neo[~df_neo["id_actividad"].isin(actividades_viejas)].copy()
        if not df_neo_nuevo.empty:
            df_neo_nuevo["tipo_actividad"] = df_neo_nuevo["tipo_actividad"].str.lower()
            fechas_actividades_limpias = df_neo["fecha_actividad"].apply(
                lambda x: x.to_native() if hasattr(x, "to_native") else x
            )
            df_neo_nuevo["id_tiempo_fk"] = pd.to_datetime(fechas_actividades_limpias, utc=True).dt.strftime("%Y%m%d%H").astype(int)
            for col in ['pais', 'region', 'ciudad']:
                df_neo_nuevo[col] = df_neo_nuevo[col].astype(str).str.strip().str.lower()         
            df_act_mapeada = df_neo_nuevo.merge(df_geografia_unificada_completa, on=['pais', 'region', 'ciudad'], how='left')
            df_act_mapeada = df_act_mapeada.merge(dim_dispositivo_completa, left_on="dispositivo", right_on="tipo", how="left")
            df_act_mapeada = df_act_mapeada.merge(dim_tipo_actividad_completa, left_on="tipo_actividad", right_on="descripcion", how="left")

            hechos_actividad = df_act_mapeada[[
                "id_actividad", "id_usuario_fk", "id_publicacion_fk", "id_tiempo_fk",
                "id_geografia", "id_dispositivo", "id_tipo_actividad",
                
            ]].rename(columns={
                "id_geografia": "id_geografia_fk",
                "id_dispositivo": "id_dispositivo_fk", 
                "id_tipo_actividad": "id_tipo_actividad_fk"
            })
            
            hechos_actividad = hechos_actividad.drop_duplicates(subset=["id_actividad"])
            df_publicacion_completa = pd.read_sql(text("SELECT id_publicacion FROM publicacion"), con=conn_dwh)
            publicaciones_validas = df_publicacion_completa["id_publicacion"].unique()
            hechos_actividad_filtrada = hechos_actividad[hechos_actividad["id_publicacion_fk"].isin(publicaciones_validas)]
            if not hechos_actividad_filtrada.empty:
                hechos_actividad_filtrada.to_sql("actividad", con=conn_dwh, if_exists="append", index=False)
                print(f"Se cargaron {len(hechos_actividad_filtrada)} actividades nuevas")             
        conn_dwh.commit()

    print("ETL de creación e inserción finalizado exitosamente.")

if __name__ == "__main__":
    etl_creacion()