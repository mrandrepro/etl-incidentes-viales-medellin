FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow
RUN pip install --no-cache-dir pandas requests pymysql sqlalchemy kafka-python
