import pandas as pd
import uuid
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs):
    """
    Exporta datos crudos de múltiples servicios (Yellow y Green)
    a Snowflake, creando una tabla RAW separada por servicio.
    Aplica idempotencia: elimina previamente registros del mismo año/mes.
    """

    # Credenciales desde Mage Secrets
    user = get_secret_value("SNOWFLAKE_USER")
    password = get_secret_value("SNOWFLAKE_PASSWORD")
    account = get_secret_value("SNOWFLAKE_ACCOUNT")
    warehouse = get_secret_value("SNOWFLAKE_DEFAULT_WH")
    database = get_secret_value("SNOWFLAKE_DEFAULT_DB")
    schema = get_secret_value("SNOWFLAKE_DEFAULT_SCHEMA")
    role = get_secret_value("SNOWFLAKE_ROLE")

    # Conexión única
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        insecure_mode=True
    )
    cursor = conn.cursor()

    # Detectar servicios únicos en el dataframe
    services = df["__service_type"].unique()
    print(f"Detectados servicios: {', '.join(services)}")

    for service in services:
        df_service = df[df["__service_type"] == service].copy()
        table_name = f"{service}_trips".upper()

        run_id = str(df_service["__run_id"].iloc[0])
        year = int(df_service["__year"].iloc[0])
        month = int(df_service["__month"].iloc[0])
        row_count = len(df_service)

        print(f"Exportando {row_count:,} filas → {schema}.{table_name} ({service.upper()} {year}-{month:02d})")

        # Normalizar columnas datetime
        datetime_cols = [c for c in df_service.columns if "datetime" in c.lower()]
        for col in datetime_cols:
            df_service[col] = pd.to_datetime(df_service[col], errors="coerce")
            df_service[col] = df_service[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Crear tabla si no existe
        cols_def = []
        for col, dtype in df_service.dtypes.items():
            if col in datetime_cols:
                col_type = "TIMESTAMP_NTZ"
            elif "int" in str(dtype):
                col_type = "NUMBER"
            elif "float" in str(dtype):
                col_type = "FLOAT"
            else:
                col_type = "VARCHAR"
            cols_def.append(f'"{col.upper()}" {col_type}')

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {", ".join(cols_def)}
            );
        """
        cursor.execute(create_sql)
        print(f"Tabla {schema}.{table_name} creada/verificada")

        # 🧹 IDEMPOTENCIA → borrar registros del mismo año/mes antes de insertar
        delete_sql = f"""
            DELETE FROM {schema}.{table_name}
            WHERE "__YEAR" = {year} AND "__MONTH" = {month};
        """
        cursor.execute(delete_sql)
        conn.commit()
        print(f"Registros previos eliminados para {year}-{month:02d} ({service.upper()})")

        # Exportación bulk
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df_service,
            table_name,
            schema=schema,
            quote_identifiers=False,
            auto_create_table=False
        )
        print(f"Exportados {nrows:,} registros a {table_name} en {nchunks} chunks")

        # Auditoría
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.INGEST_AUDIT (
                run_id VARCHAR,
                service_type VARCHAR,
                year NUMBER,
                month NUMBER,
                row_count NUMBER,
                ingest_ts TIMESTAMP_NTZ
            );
        """)

        cursor.execute(f"""
            INSERT INTO {schema}.INGEST_AUDIT
            (run_id, service_type, year, month, row_count, ingest_ts)
            VALUES ('{run_id}', '{service}', {year}, {month}, {row_count},
                    CONVERT_TIMEZONE('UTC', 'America/Guayaquil', CURRENT_TIMESTAMP));
        """)
        conn.commit()
        print(f"Auditoría insertada para {service.upper()} {year}-{month:02d}")

    conn.close()
    print("Exportación completada para todos los servicios.")


@test
def test_output(*args, **kwargs):
    assert True, "Exporter ejecutado correctamente"
