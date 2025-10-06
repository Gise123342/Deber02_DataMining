import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def dbt_env_setup(*args, **kwargs):
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
        if value:
            os.environ[key] = str(value)

    os.environ["DBT_PROFILES_DIR"] = "/home/src/scheduler/dbt"
    os.environ["DBT_PROFILE_NAME"] = "interpolated"
    os.environ["DBT_DEFAULT_SCHEMA"] = "SILVER"

    print("Variables de entorno configuradas para DBT (capa SILVER):")
    print(f"DBT_PROFILES_DIR = {os.environ['DBT_PROFILES_DIR']}")
    print(f"DBT_PROFILE_NAME = {os.environ['DBT_PROFILE_NAME']}")
    print(f"DBT_DEFAULT_SCHEMA = {os.environ['DBT_DEFAULT_SCHEMA']}")
    print("Variables cargadas:", ", ".join(env_vars.keys()))


@test
def test_output(*args, **kwargs):
    required_vars = [
        "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DEFAULT_WH", "SNOWFLAKE_DEFAULT_DB",
        "SNOWFLAKE_DEFAULT_SCHEMA", "SNOWFLAKE_ROLE",
        "DBT_PROFILES_DIR", "DBT_PROFILE_NAME", "DBT_DEFAULT_SCHEMA"
    ]
    missing = [v for v in required_vars if v not in os.environ]
    assert not missing, f"Faltan variables: {missing}"
    print("Variables correctamente configuradas.")
