import pandas as pd
import os
from sqlalchemy import create_engine, text
import urllib.parse

PASS_DWH_TEXTO = "Datawarehouse.vbs"
PASS_SOURCE_TEXTO  = "Datamart.vbs"

pass_dwh_segura = urllib.parse.quote_plus(PASS_DWH_TEXTO)
pass_source_segura  = urllib.parse.quote_plus(PASS_SOURCE_TEXTO)

URL_DATAWAREHOUSE = f"postgresql://postgres.gjpqfmnbabmjohobaxfv:{pass_dwh_segura}@aws-1-us-east-1.pooler.supabase.com:6543/postgres?options=-c%20project=gjpqfmnbabmjohobaxfv"
URL_SOURCE      = f"postgresql://postgres.nksiwgmgejhbkyyftnub:{pass_source_segura}@aws-1-us-east-2.pooler.supabase.com:6543/postgres?options=-c%20project=nksiwgmgejhbkyyftnub"

engine_source = create_engine(URL_SOURCE)
engine_dwh = create_engine(URL_DATAWAREHOUSE)

def sincronizar_usuarios():
    
    #Extract
    query = text("SELECT * FROM usuario")
    df_dwh = pd.read_sql(query, con=engine_dwh)
    df_source = pd.read_sql(query, con=engine_source)

    ### Transform
    #Datos nuevos
    df_dwh.set_index('id_usuario', inplace=False)
    df_source.set_index('id_usuario', inplace=False)
    nuevos_ids = set(df_source['id_usuario']) - set(df_dwh['id_usuario'])
    df_nuevos = df_dwh[df_source['id_usuario'].isin(nuevos_ids)]
    #Datos comunes
    ids_comunes = set(df_source['id_usuario']).intersection(set(df_dwh['id_usuario']))   
    df_dwh_comunes = df_dwh[df_dwh['id_usuario'].isin(ids_comunes)].sort_values('id_usuario').reset_index(drop=True)
    df_source_comunes = df_source[df_source['id_usuario'].isin(ids_comunes)].sort_values('id_usuario').reset_index(drop=True)
    #Ordenar columnas
    columnas = [col for col in df_dwh.columns]
    df_dwh_comunes = df_dwh_comunes[columnas]
    df_source_comunes = df_source_comunes[columnas]
    #Capturamos los diferentes
    cambiados_mask = ~(df_source_comunes.isin(df_dwh_comunes).all(axis=1))
    df_actualizar = df_source_comunes[cambiados_mask]


    ###Load
    #Nuevos
    if not df_nuevos.empty:
        df_nuevos.to_sql('usuario', con=engine_dwh, if_exists='append', index=False)
        print("Inserciones finalizadas.")
    else:
        print("No se detectaron usuarios nuevos.")
    #Actualizados
    if not df_actualizar.empty:
        print(f"Actualizando {len(df_actualizar)} usuarios modificados")
        with engine_dwh.begin() as conexion:
            query_update = text("""
                UPDATE usuario 
                SET nombre_usuario = :nombre_usuario,
                    mail = :mail,
                    suscription = :suscription,
                    edad = :edad,
                    genero = :genero,
                    seguidores = :seguidores,
                    seguidos = :seguidos
                WHERE id_usuario = :id_usuario
            """)
            parametros = df_actualizar.to_dict(orient='records')
            conexion.execute(query_update, parametros)
            
        print("Actualizaciones finalizadas con éxito.")
    else:
        print("No se encontraron cambios en los usuarios existentes.")

if __name__ == "__main__":
    sincronizar_usuarios()