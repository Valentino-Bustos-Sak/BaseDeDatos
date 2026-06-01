import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
# ============================================================================
# 1. CONFIGURACIÓN DE CONEXIONES
# ============================================================================
# Cambiá las credenciales por las de tus bases de datos en Supabase / Postgres

load_dotenv()  # Levanta el archivo .env
PASSWORD_DM = os.getenv("Supa_Password_DM")
PASSWORD_DW = os.getenv("Supa_Password_DW")

URL_DATAWAREHOUSE = f"postgresql://postgres.gjpqfmnbabmjohobaxfv:{PASSWORD_DW}@aws-1-us-east-1.pooler.supabase.com:6543/postgres?options=-c%20project=gjpqfmnbabmjohobaxfv"
URL_DATAMART      = f"postgresql://postgres.nksiwgmgejhbkyyftnub:{PASSWORD_DM}@aws-1-us-east-2.pooler.supabase.com:6543/postgres?options=-c%20project=nksiwgmgejhbkyyftnub"

engine_dwh = create_engine(URL_DATAWAREHOUSE)
engine_mart = create_engine(URL_DATAMART)

def sincronizar_usuarios():
    print("🚀 Iniciando proceso de sincronización de usuarios...")

    # ============================================================================
    # 2. EXTRACT: Leer ambas tablas
    # ============================================================================
    print("📥 Extrayendo datos de Origen (DWH) y Destino (Data Mart)...")
    
    # Traemos los usuarios del Data Warehouse
    df_dwh = pd.read_sql("SELECT * FROM usuario", con=engine_dwh)
    
    # Traemos los usuarios actuales del Data Mart
    df_mart = pd.read_sql("SELECT * FROM usuario", con=engine_mart)

    # Si el Data Mart está vacío, subimos todo directamente y terminamos
    if df_mart.empty:
        print("ℹ️ El Data Mart está vacío. Cargando todos los usuarios por primera vez...")
        df_dwh.to_sql('usuario', con=engine_mart, if_exists='append', index=False)
        print(f"✅ Se insertaron {len(df_dwh)} usuarios iniciales.")
        return

    # ============================================================================
    # 3. TRANSFORM: Comparar y detectar cambios
    # ============================================================================
    print("🧠 Analizando diferencias y actualizaciones...")
    
    # Forzar que el ID sea el índice para facilitar la comparación
    df_dwh.set_index('id_usuario', inplace=False)
    df_mart.set_index('id_usuario', inplace=False)

    # A. DETECTAR NUEVOS: Están en DWH pero no en el Data Mart
    nuevos_ids = set(df_dwh['id_usuario']) - set(df_mart['id_usuario'])
    df_nuevos = df_dwh[df_dwh['id_usuario'].isin(nuevos_ids)]

    # B. DETECTAR ACTUALIZACIONES: Existen en ambos pero algo cambió
    ids_comunes = set(df_dwh['id_usuario']).intersection(set(df_mart['id_usuario']))
    
    # Filtramos ambos DataFrames para quedarnos solo con los registros que coexisten
    df_dwh_comunes = df_dwh[df_dwh['id_usuario'].isin(ids_comunes)].sort_values('id_usuario').reset_index(drop=True)
    df_mart_comunes = df_mart[df_mart['id_usuario'].isin(ids_comunes)].sort_values('id_usuario').reset_index(drop=True)

    # Aseguramos que las columnas estén en el mismo orden exacto para comparar las filas limpiamente
    columnas = [col for col in df_dwh.columns]
    df_dwh_comunes = df_dwh_comunes[columnas]
    df_mart_comunes = df_mart_comunes[columnas]

    # Creamos una máscara booleana: compara fila a fila todo el registro
    # Si alguna celda difiere, la fila da False. Con el `~` invertimos para capturar los modificados.
    cambiados_mask = ~(df_dwh_comunes.isin(df_mart_comunes).all(axis=1))
    df_actualizar = df_dwh_comunes[cambiados_mask]

    # ============================================================================
    # 4. LOAD: Aplicar los cambios en el Data Mart
    # ============================================================================
    
    # Acción A: Insertar los nuevos registros
    if not df_nuevos.empty:
        print(f"➕ Insertando {len(df_nuevos)} usuarios nuevos en el Data Mart...")
        df_nuevos.to_sql('usuario', con=engine_mart, if_exists='append', index=False)
        print("✅ Inserciones finalizadas.")
    else:
        print("ℹ️ No se detectaron usuarios nuevos.")

    # Acción B: Actualizar los registros modificados
    if not df_actualizar.empty:
        print(f"🔄 Actualizando {len(df_actualizar)} usuarios modificados en el Data Mart...")
        
        # Como SQLAlchemy/Pandas no tiene un método directo `.to_sql(if_exists='update')`, 
        # ejecutamos un bucle UPDATE optimizado usando una transacción de SQL nativo
        with engine_mart.begin() as conexion:
            query_update = text("""
                UPDATE usuario 
                SET nombre_usuario = :nombre_usuario,
                    mail = :mail,
                    suscripto = :suscripto,
                    edad = :edad,
                    genero = :genero,
                    seguidores = :seguidores,
                    seguidos = :seguidos
                WHERE id_usuario = :id_usuario
            """)
            
            # Convertimos el DataFrame a una lista de diccionarios que recibe la query
            parametros = df_actualizar.to_dict(orient='records')
            conexion.execute(query_update, parametros)
            
        print("✅ Actualizaciones finalizadas con éxito.")
    else:
        print("ℹ️ No se encontraron cambios en los usuarios existentes.")

if __name__ == "__main__":
    sincronizar_usuarios()