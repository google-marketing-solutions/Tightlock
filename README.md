# Tightlock - First-Party Data Tool

## Useful commands

`docker-compose up -d --build` - Initializes application detached and rebuilds containers

`docker logs tightlock-api` - Shows API logs only

`docker-compose down -v` - Cleans up containers and remove all volumes

`docker exec -ti tightlock_tightlock-api_1 bash` - Inspects the API container (useful to run migrations)

- `alembic revision --autogenerate -m "Some message"` - Creates a new revision with the changes made to the data models
- `alembic upgrade head` - Applies migrations to DB

`docker exec -ti tightlock_postgres_1 psql -U tightlock -W tightlock` - Runs Postgres REPL inside DB container (useful for inspecting tables)

## Deployong on Google Compute Engine 

> TODO(b/265932821): Change for installer instructions once installer is available

- Change exposed port for `tightlock_api` from `8081` to `80` on the `docker-compose.yaml` file.

- Turn down all containers by running `docker run -v /var/run/docker.sock:/var/run/docker.sock --rm -v "$PWD:$PWD" -w "$PWD" docker/compose:1.29.2 down -v`

- Modify `run_tightlock.sh` script, changing references to the `docker-compose` command to `docker run -v /var/run/docker.sock:/var/run/docker.sock --rm -v "$PWD:$PWD" -w "$PWD" docker/compose:1.29.2`

- Run `bash run_tightlock.sh` and provide or create an API key
