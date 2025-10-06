FROM mageai/mageai:latest

WORKDIR /home/src

COPY scheduler_data/scheduler/requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENV DBT_PROFILES_DIR=/home/src/scheduler/dbt
