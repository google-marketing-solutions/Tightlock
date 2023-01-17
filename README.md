# Tightlock - First-Party Data Tool

## Useful commands

`docker-compose up -d --build` - Initializes application detached and rebuilds containers

`docker logs tightlock-api` - Shows API logs only

`docker-compose down -v` - Cleans up containers and remove all volumes

`docker exec -ti tightlock_tightlock-api_1 bash` - Inspects the API container (useful to run migrations)

- `alembic revision --autogenerate -m "Some message"` - Creates a new revision with the changes made to the data models
- `alembic upgrade head` - Applies migrations to DB

`docker exec -ti tightlock_postgres_1 psql -U tightlock -W tightlock` - Runs Postgres REPL inside DB container (useful for inspecting tables)

