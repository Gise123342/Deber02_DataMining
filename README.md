# Proyecto: NYC Taxi Data Warehouse – Mage + DBT + Snowflake  

**Estudiante:** Giselle Cevallos  
**Fecha:** Octubre 2025  

---

## Descripción y arquitectura general

El proyecto implementa una arquitectura de datos moderna para el análisis de viajes de taxis de Nueva York (**NYC Taxi Trips**), utilizando **Mage AI** como orquestador, **DBT** como framework de transformación y **Snowflake** como Data Warehouse.

El flujo de procesamiento sigue el modelo clásico de tres capas:

| Capa | Descripción | Objetivo |
|------|--------------|----------|
| **RAW (Bronze)** | Ingesta directa de fuentes originales en Snowflake. | Mantener datos sin transformar. |
| **SILVER (Silver)** | Limpieza, filtrado y estandarización. | Crear una base confiable y unificada. |
| **GOLD (Gold)** | Modelado analítico dimensional. | Facilitar análisis e indicadores. |

### Orquestación en Mage
1. Se realiza la carga de datos crudos en la tuberia raw, creando una conexion con la bdd en snowflake, obteniendo datos de NYC (data loader) y exportandolos a snowflake (export)
2. En la tuberia Silver se realiza la utilizacion de los datos crudos exportados en raw y se utiliza metodos de dbt para crear los nuevos datos. Ademas se convinan tablas yellow y green en una misma tabla. Se utiliza dbt y bloques custom para definir parametros y probar tests
3. Por ultimo en la capa gold. Se realiza el uso de dimensionalidades con dbt y la creacion de una tabla de hechos para generar un modelo en estrella
   
### Diagrama de arquitectura
<img width="430" height="414" alt="image" src="https://github.com/user-attachments/assets/61a446fe-84fa-4a39-9658-1afb641809fb" />
<img width="1020" height="643" alt="image" src="https://github.com/user-attachments/assets/655cf66e-37d6-4e85-9c75-286b43244377" />

## Cobertura de meses 2015–2025 (matriz por servicio) y estado de carga (Parquet).
Contenemos dos auditoria. Una en la capa raw que se encarga de documentar la carga inicial y la auditoria silver que se encarga de documentar cargas de limpieza
Se tuvo problemas con la carga de mas meses y años. Por ende, solo se realiza de un mes. Todo el proyecto tiene integrado todos los requerimientos para un mes
<img width="1215" height="537" alt="image" src="https://github.com/user-attachments/assets/ee397768-7377-4371-bcb6-ca0ffc16891c" />
<img width="1264" height="507" alt="image" src="https://github.com/user-attachments/assets/4e1717d7-87b2-46f2-a519-9f800a467ba7" />
## Estrategia de pipeline de backfill mensual e idempotencia.
El pipeline RAW fue diseñado para asegurar idempotencia total y permitir backfill mensual controlado.  
Esto garantiza que cada ejecución del pipeline es segura y reproducible, sin generar duplicados ni inconsistencias.
Antes de insertar nuevos registros en Snowflake, el exportador ejecuta una eliminación selectiva:
<img width="921" height="336" alt="image" src="https://github.com/user-attachments/assets/6c539454-4548-4f13-a7fd-4b095af683ad" />
<img width="362" height="111" alt="image" src="https://github.com/user-attachments/assets/2a598186-c3fa-4114-bcfc-43d9f3c99b5c" />
## Gestión de secretos (nombres y propósito) y cuenta de servicio / rol (permisos mínimos).

Se utiliza Secrets para todas las credenciales del user:
<img width="674" height="539" alt="image" src="https://github.com/user-attachments/assets/96d878b6-39ae-4b7d-a485-ddcae571e11a" />
Se utiliza secrets en todos los accesos a la bdd tanto por exporter como en raw como en los costums en silver y gold:
<img width="596" height="221" alt="image" src="https://github.com/user-attachments/assets/a0a642ef-dd19-49e9-9571-6cd58a171b5c" />
<img width="706" height="458" alt="image" src="https://github.com/user-attachments/assets/7737521b-9784-450b-904d-408bba31a221" />
<img width="772" height="335" alt="image" src="https://github.com/user-attachments/assets/aef9d9b1-368b-4dab-a1a3-dd9f7742b681" />
Ademas todo se gestiona por un usuario administrador adicional para que tenga acceso y manipulacion a la bdd:
<img width="1493" height="799" alt="image" src="https://github.com/user-attachments/assets/9bb1b4e3-dcd6-4e90-856a-01b830689393" />

## Diseño de silver (reglas de limpieza/estandarización) y gold (hechos/dimensiones).

### Capa SILVER

La capa SILVER se encarga de la limpieza, estandarización y enriquecimiento de los datos provenientes de la capa RAW.  
Los modelos TRIPS_CLEAN, TRIPS_CLEAN_GREEN y TRIPS_CLEAN_ALL consolidan los viajes de taxi amarillos y verdes, aplicando reglas de calidad y consistencia de negocio.

**Reglas de limpieza y estandarización aplicadas**

| Regla | Descripción |
|-------|--------------|
| Eliminación de registros inválidos | Se descartan registros con fare_amount <= 0, trip_distance <= 0, o total_amount <= 0. |
|  Restricción de pasajeros | Solo se conservan viajes con passenger_count BETWEEN 1 AND 8. |
| Validación temporal | Se asegura que dropoff_ts >= pickup_ts y que la duración del viaje no exceda 12 horas (DATEDIFF(hour, pickup_ts, dropoff_ts) <= 12). |
| Conversión de tipos | Se aplican castings explícitos (CAST, ::INT, ::FLOAT, ::TIMESTAMP_NTZ) para uniformar los tipos de datos. |
| Enriquecimiento geográfico | Se realiza un LEFT JOIN con TAXI_ZONE_LOOKUP para obtener pickup_zone, pickup_borough, dropoff_zone, y dropoff_borough. |
| Cálculo de propinas | Se genera la columna `tip_percent = ROUND((tip_amount / NULLIF(fare_amount, 0)) * 100, 2). |
| Control de carga | Se añade la columna load_ts = CURRENT_TIMESTAMP() para rastrear el momento de la ingesta. |

### Capa GOLD

La capa GOLD representa el nivel semántico y analítico del modelo, implementando una arquitectura Star Schema (modelo en estrella) mediante DBT.  
Incluye siete dimensiones conformadas y una tabla de hechos principal (FACT_TRIPS) con granularidad a nivel de viaje.

**Estructura del modelo dimensional**

| Dimensión        | Descripción                                          | Fuente                                     |
| ---------------- | ---------------------------------------------------- | ------------------------------------------ |
| DIM_DATE         | Fechas con jerarquías de año, mes, día y tipo de día | Derivada de pickup_ts                      |
| DIM_ZONE         | Zonas geográficas y boroughs                         | Taxi Zone Lookup                           |
| DIM_VENDOR       | Proveedores de servicio                              | VendorID                                   |
| DIM_RATE_CODE    | Códigos tarifarios TLC                               | Tabla estática de referencia               |
| DIM_PAYMENT_TYPE | Métodos de pago                                      | Columna payment_type proveniente de SILVER |
| DIM_SERVICE_TYPE | Tipo de servicio (yellow / green)                    | Campo service_type                         |
| DIM_TRIP_TYPE    | Tipo de viaje (street-hail o dispatch)               | Tabla estática de referencia               |

**Tabla de hechos FACT_TRIPS**

| Atributo        | Descripción                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------ |
| Grano           | Una fila por cada viaje (1 viaje = 1 fila)                                                                         |
| Claves foráneas | Referencias a todas las dimensiones GOLD                                                                           |
| Métricas        | trip_distance, trip_minutes, fare_amount, tip_amount, total_amount, avg_tip_percent, total_revenue                 |
| Agregaciones    | Se generan métricas por año, mes, borough y zona (GROUP BY pickup_year, pickup_month, pickup_borough, pickup_zone) |

- Alineación total con principios de modelado dimensional Kimball.  
- Separación clara entre datos limpios (SILVER) y analíticos (GOLD).  
- Escalabilidad y compatibilidad con Snowflake para consultas optimizadas.  
- Posibilidad de análisis temporal y geográfico con un modelo consistente y documentado.

## Clustering: llaves elegidas, métricas antes/después, conclusiones.
ara optimizar el rendimiento analítico se definió clustering automático sobre las columnas:
pickup_year
pickup_borough
Snowflake permite definir claves de clustering (Cluster Keys) en tablas grandes
para mejorar el rendimiento de consultas filtradas o agregadas por esas columnas.
<img width="771" height="268" alt="image" src="https://github.com/user-attachments/assets/77f7230e-cc39-42cd-aa03-0e84f50dac79" />
Esto crea tu tabla GOLD.FACT_TRIPS con clustering físico por esas tres columnas,
optimizando las consultas más comunes (por año, mes y borough).
