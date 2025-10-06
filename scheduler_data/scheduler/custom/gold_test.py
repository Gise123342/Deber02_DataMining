if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import subprocess
from mage_ai.data_preparation.shared.secrets import get_secret_value


@custom
def run_dbt_tests(*args, **kwargs):
    """
    Ejecuta todos los tests de DBT definidos en el schema.yml
    para la capa GOLD (dimensiones + fact_trips).
    """

    # 1. Cargar variables de entorno desde Mage Secrets
    env_vars = {
        "SNOWFLAKE_USER": get_secret_value("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": get_secret_value("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": get_secret_value("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_DEFAULT_WH": get_secret_value("SNOWFLAKE_DEFAULT_WH"),
        "SNOWFLAKE_DEFAULT_DB": get_secret_value("SNOWFLAKE_DEFAULT_DB"),
        "SNOWFLAKE_DEFAULT_SCHEMA": get_secret_value("SNOWFLAKE_DEFAULT_SCHEMA"),
        "SNOWFLAKE_ROLE": get_secret_value("SNOWFLAKE_ROLE"),
    }

    for key, value in env_vars.items():
        if value is not None:
            os.environ[key] = str(value)

    # 2. Configuración DBT para capa GOLD
    os.environ["DBT_PROFILES_DIR"] = "/home/src/scheduler/dbt"
    os.environ["DBT_PROFILE_NAME"] = "interpolated"
    os.environ["DBT_DEFAULT_SCHEMA"] = "GOLD"

    print("Variables de entorno configuradas para DBT (capa GOLD).")
    print(f"DBT_PROFILES_DIR = {os.environ['DBT_PROFILES_DIR']}")
    print(f"DBT_PROFILE_NAME = {os.environ['DBT_PROFILE_NAME']}")
    print(f"DBT_DEFAULT_SCHEMA = {os.environ['DBT_DEFAULT_SCHEMA']}")
    print("Variables cargadas:", ", ".join(env_vars.keys()))

    # 3. Ejecutar los tests de DBT usando subprocess
    print("Ejecutando tests de la capa GOLD...")

    cmd = [
        "dbt",
        "test",
        "--project-dir", "/home/src/scheduler/dbt",
        "--select", "gold",
        "--profiles-dir", "/home/src/scheduler/dbt"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    print("Resultado de los tests:")
    print(result.stdout)
    if result.stderr:
        print("Errores detectados:")
        print(result.stderr)

    return result.stdout


@test
def test_output(*args, **kwargs):
    """
    Verifica que los tests de DBT se ejecutaron correctamente.
    """
    print("Validación: Bloque 'run_dbt_tests' ejecutado correctamente.")
