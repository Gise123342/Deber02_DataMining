import pandas as pd
import uuid

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Carga datos de Yellow y Green taxis para el año y mes indicados.
    Devuelve un único DataFrame concatenado con columna __service_type.
    """
    year = kwargs.get("year", 2024)
    month = kwargs.get("month", 4)

    # Servicios a cargar
    services = ["yellow", "green"]

    all_dfs = []

    for service in services:
        file_name = f"{service}_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        print(f"⬇Cargando datos {service.upper()} de {year}-{month:02d} desde {url}")

        try:
            df = pd.read_parquet(url)
        except Exception as e:
            print(f"No se pudo cargar {service.upper()} {year}-{month:02d}: {e}")
            continue

        # Metadatos
        run_id = str(uuid.uuid4())
        df.attrs["run_id"] = run_id
        df.attrs["year"] = year
        df.attrs["month"] = month
        df.attrs["service_type"] = service
        df.attrs["row_count"] = len(df)

        df["__run_id"] = run_id
        df["__year"] = year
        df["__month"] = month
        df["__service_type"] = service

        all_dfs.append(df)

    if not all_dfs:
        raise ValueError("No se pudo cargar ningún dataset (verifica URLs o meses).")

    # Unir todos los servicios
    combined_df = pd.concat(all_dfs, ignore_index=True)

    print(f"Cargados {len(combined_df):,} registros totales "
          f"de {len(all_dfs)} servicios ({', '.join(services)})")

    return combined_df


@test
def test_output(output, *args) -> None:
    assert output is not None, "El output es None"
    assert len(output) > 0, "El DataFrame está vacío"
    assert "__service_type" in output.columns, "Falta columna __service_type"
