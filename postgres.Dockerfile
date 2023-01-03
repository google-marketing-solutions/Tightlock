FROM postgres:15
COPY postgres_init.sh /docker-entrypoint-initdb.d/