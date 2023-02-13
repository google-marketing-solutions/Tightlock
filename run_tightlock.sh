echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up --build -d