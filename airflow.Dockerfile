# syntax=docker/dockerfile:1
FROM apache/airflow:2.5.0
COPY airflow_requirements.txt requirements.txt
RUN pip install -r requirements.txt