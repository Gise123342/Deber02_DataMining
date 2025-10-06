import os
import pandas as pd
import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value
import math

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(*args, **kwargs):
    """
    Descarga el CSV Taxi Zone Lookup y lo carga en Snowflake -> RAW.TAXI_ZONE_LOOKUP
    Sin usar write_pandas (evita OCSP y S3)
    """

    # 1️⃣ Descargar CSV
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    df = pd.read_csv(url)
    df.columns = [c.strip().upper() for c in df.columns]

    print(f"📥 CSV cargado con {len(df)} filas y columnas: {list(df.columns)}")

    # 2️⃣ Limpiar NaN → None (para SQL NULL)
    df = df.where(pd.notnull(df), None)

    # 3️⃣ Conectar a Snowflake
    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_DEFAULT_WH"),
        database=get_secret_value("SNOWFLAKE_DEFAULT_DB"),
        schema="RAW",
        role=get_secret_value("SNOWFLAKE_ROLE"),
        ocsp_fail_open=True,
        insecure_mode=True
    )
    cursor = conn.cursor()

    # 4️⃣ Crear tabla (si no existe)
    cursor.execute("""
        CREATE OR REPLACE TABLE RAW.TAXI_ZONE_LOOKUP (
            LOCATIONID INT,
            BOROUGH STRING,
            ZONE STRING,
            SERVICE_ZONE STRING
        );
    """)
    print("📦 Tabla RAW.TAXI_ZONE_LOOKUP lista para carga.")

    # 5️⃣ Insertar datos por lotes
    insert_sql = """
        INSERT INTO RAW.TAXI_ZONE_LOOKUP (LOCATIONID, BOROUGH, ZONE, SERVICE_ZONE)
        VALUES (%s, %s, %s, %s)
    """

    batch_size = 100
    records = [tuple(x) for x in df.to_numpy().tolist()]
    total = len(records)

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"✅ Insertado lote {i // batch_size + 1} ({len(batch)} filas)")

    cursor.close()
    conn.close()

    print(f"🎉 Carga completada: {total} filas insertadas en RAW.TAXI_ZONE_LOOKUP")
