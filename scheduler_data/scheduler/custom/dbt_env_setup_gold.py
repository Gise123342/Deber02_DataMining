import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def dbt_env_setup(*args, **kwargs):
    """
    Carga variables de entorno para DBT desde los Secrets de Mage
    y configura el schema dinámico GOLD.
    """

    # Variables desde Mage Secrets
    env_vars = {
        "SNOWFLAKE_USER": get_secret_value("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": get_secret_value("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": get_secret_value("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_DEFAULT_WH": get_secret_value("SNOWFLAKE_DEFAULT_WH"),
        "SNOWFLAKE_DEFAULT_DB": get_secret_value("SNOWFLAKE_DEFAULT_DB"),
        "SNOWFLAKE_DEFAULT_SCHEMA": get_secret_value("SNOWFLAKE_DEFAULT_SCHEMA"),
        "SNOWFLAKE_ROLE": get_secret_value("SNOWFLAKE_ROLE"),
    }

    # Cargar al entorno
    for key, value in env_vars.items():
        if value is not None:
            os.environ[key] = str(value)

    #  Forzar variables de entorno para DBT
    os.environ["DBT_PROFILES_DIR"] = "/home/src/scheduler/dbt"
    os.environ["DBT_PROFILE_NAME"] = "interpolated"

    # Definir el schema GOLD solo para esta tubería
    os.environ["DBT_DEFAULT_SCHEMA"] = "GOLD"

    print("Variables de entorno configuradas para DBT (capa GOLD).")
    print(f"DBT_PROFILES_DIR = {os.environ['DBT_PROFILES_DIR']}")
    print(f"DBT_PROFILE_NAME = {os.environ['DBT_PROFILE_NAME']}")
    print(f" DBT_DEFAULT_SCHEMA = {os.environ['DBT_DEFAULT_SCHEMA']}")
    print("Variables cargadas:", ", ".join(env_vars.keys()))


@test
def test_output(*args, **kwargs):
    """
    Valida que las variables estén correctamente configuradas
    antes de ejecutar modelos DBT.
    """
    required_vars = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DEFAULT_WH",
        "SNOWFLAKE_DEFAULT_DB",
        "SNOWFLAKE_DEFAULT_SCHEMA",
        "SNOWFLAKE_ROLE",
        "DBT_PROFILES_DIR",
        "DBT_PROFILE_NAME",
        "DBT_DEFAULT_SCHEMA",  
    ]

    missing = [v for v in required_vars if v not in os.environ]
    assert not missing, f"Faltan variables en entorno: {missing}"
    print("Todas las variables necesarias están configuradas correctamente para GOLD.")
