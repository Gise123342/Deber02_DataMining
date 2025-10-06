from sqlalchemy import create_engine, text

engine = create_engine(
    f"snowflake://{user}:{password}@alnwmmv-nj56428.sa-east-1.aws/{database}/{schema}?warehouse={warehouse}&role={role}"
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT CURRENT_VERSION()")).fetchone()
    print("✅ Conectado a Snowflake. Versión:", result[0])
