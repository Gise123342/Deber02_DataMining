import subprocess
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def run_dbt_tests(*args, **kwargs):
    project_dir = "/home/src/scheduler/dbt"
    command = [
        "dbt", "test",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
        "--select", "silver.*"
    ]

    print(f" Ejecutando DBT tests en {project_dir}...")
    result = subprocess.run(command, capture_output=True, text=True)

    print(" STDOUT:\n", result.stdout)
    print(" STDERR:\n", result.stderr)

    if result.returncode == 0:
        print(" Tests completados correctamente.")
    else:
        raise Exception(" Error en los tests DBT.")
