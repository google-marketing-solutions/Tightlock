"""
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""Entrypoint for FastAPI application."""
import os
import contextlib
import json
from typing import Annotated, Any

from clients import AirflowClient
from db import get_session
from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import Response, JSONResponse
from models import (Config, ConfigValue, Connection, ConnectResponse, RunLogsResponse,
                    ValidationResult)
from security import check_authentication_header
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette import status

# Creates base app and v1 API objects
app = FastAPI()
v1 = FastAPI(dependencies=[Depends(check_authentication_header)])


@app.on_event("startup")
async def create_initial_config():
  """Initializes the system with an initial config when it is not found."""
  f = open("base_config.json")
  data = json.load(f)
  get_session_wrapper = contextlib.asynccontextmanager(get_session)
  async with get_session_wrapper() as session:
    try:
      await create_config(Config(label="Initial Config",
                                 value=data), session=session)
    except HTTPException:
      pass  # Ignore workers trying to recreate initial config


@v1.post("/connect", response_model=ConnectResponse)
async def connect():
  """Validates API connection with a client."""
  latest_tag = os.environ.get("LATEST_TAG")
  return ConnectResponse(version=latest_tag)

# TODO(b/290388517): Remove mentions to activation once UI is ready
@v1.post("/connections/{connection_name}:trigger")
@v1.post("/activations/{activation_name}:trigger")
async def trigger_connection(
    activation_name: str | None = None,
    connection_name: str | None = None,
    dry_run: bool = Body(..., embed=True),
    airflow_client=Depends(AirflowClient),
) -> Response:
  """Triggers a connection identified by name.
  
  Args:
    activation_name: The name of the target connection to trigger (legacy)
    connection_name: The name of the target connection to trigger.
    dry_run: A boolean indicating whether or not to actually send
      the data to the underlying destination or if it should only
      be validated (when validation is available).
    airflow_client: Airflow Client dependency injection.
  Returns:
    An HTTP response that indicates success of the trigger request
    via status_code.

  """
  final_connection_name = activation_name or connection_name
  trigger_conf = {"dry_run": dry_run}
  response = await airflow_client.trigger(final_connection_name,
                                          conf=trigger_conf)
  return Response(status_code=response.status_code)


@v1.get("/configs", response_model=list[Config])
async def get_configs(session: AsyncSession = Depends(get_session)):
  """Retrieves stored configs.
  
  Args:
    session: Postgres DB session dependency injection.
  Returns:
    A List of stored configs wrapped in an HTTP JSONResponse.
  """
  result = await session.execute(select(Config))
  configs = result.scalars().all()
  return [
      Config(
          create_date=config.create_date,
          label=config.label,
          value=config.value,
          id=config.id,
      )
      for config in configs
  ]


@v1.get("/configs:getLatest", response_model=Config)
async def get_latest_config(session: AsyncSession = Depends(get_session)):
  """Retrieves the most recent config.
  
  Args:
    session: Postgres DB session dependency injection.
  Returns:
    The latest config that was created wrapped in an HTTP JSONResponse.
  """
  statement = select(Config).order_by(Config.create_date.desc()).limit(1)
  result = await session.execute(statement)
  try:
    row = result.one()[0]
  except NoResultFound:
    return Response(status_code=404)
  return Config(
      create_date=row.create_date,
      label=row.label,
      value=row.value,
      id=row.id,
  )


@v1.get("/configs/{config_id}", response_model=Config)
async def get_config(config_id: int,
                     session: AsyncSession = Depends(get_session)):
  """Retrieves a config with the provided id.
  
  Args:
    config_id: integer that uniquely identifies a config 
    session: Postgres DB session dependency injection.
  Returns:
    The target config wrapped in an HTTP JSONResponse.
  """
  statement = select(Config).where(Config.id == config_id)
  result = await session.execute(statement)
  try:
    config = result.one()[0]
  except NoResultFound:
    return Response(status_code=404)
  return Config(
      id=config.id,
      create_date=config.create_date,
      label=config.label,
      value=config.value,
  )


@v1.post("/configs", response_model=Config)
async def create_config(config: Config,
                        session: AsyncSession = Depends(get_session)):
  """Creates a new config using the provided config object.
  
  Args:
    config: config object to be stored. 
    session: Postgres DB session dependency injection.
  Returns:
    The stored config wrapped in an HTTP JSONResponse.
  Raises:
    HTTPException: A 409 error when the provided config label already exists. 
  """
  session.add(config)
  try:
    await session.commit()
  except IntegrityError as exc:  # Raised when label uniqueness is violated.
    raise HTTPException(
        status_code=409, detail=f"Config label {config.label} already exists."
    ) from exc
  return config


# TODO(b/290388517): Remove mentions to activation once UI is ready
@v1.get("/connections", response_model=list[Connection])
@v1.get("/activations", response_model=list[Connection])
async def get_connections(session: AsyncSession = Depends(get_session)):
  """Queries latest config and query connections field from config json.
  
  Args:
    session: Postgres DB session dependency injection.
  Returns:
    The connections registered in the latests config wrapped in an HTTP JSONResponse.
  """
  latest_config = await get_latest_config(session=session)
  connections = [Connection(**a) for a in latest_config.value["activations"]]
  return connections


@v1.get("/schemas")
async def get_schemas(airflow_client=Depends(AirflowClient)):
  """Retrieves available schemas for Connections.

  Args:
    airflow_client: Airflow Client dependency injection.
  
  Returns:
    A JSONSchema representing all sources and destinations schemas available.
  """
  response = await airflow_client.get_schemas()
  if not response:
    return JSONResponse(status_code=404)
  return JSONResponse(content=response)


@v1.post("/sources/{source_name}:validate", response_model=ValidationResult)
async def validate_source(
    source_name: str,
    source_config: ConfigValue,
    airflow_client=Depends(AirflowClient),
):
  """Validates the provided source config.
  
  Args:
    source_name: The name of the source type to validate.
    source_config: The ConfigValue object to be validated.
    airflow_client: Airflow Client dependency injection.
  Returns:
    The ValidationResult object wrapped in an HTTP JSONResponse.
  """
  response = await airflow_client.validate_source(
      source_name.lower(), source_config.value
  )
  return response


@v1.post("/destinations/{destination_name}:validate", response_model=ValidationResult)
async def validate_destination(
    destination_name: str,
    destination_config: ConfigValue,
    airflow_client=Depends(AirflowClient),
):
  """Validates the provided destination config.
  
  Args:
    destination_name: The name of the destination type to validate.
    destination_config: The ConfigValue object to be validated.
    airflow_client: Airflow Client dependency injection.
  Returns:
    The ValidationResult object wrapped in an HTTP JSONResponse.
  """
  response = await airflow_client.validate_destination(
      destination_name.lower(), destination_config.value
  )
  return response


# TODO(b/290388517): Remove mentions to activation once UI is ready
@v1.get("/connections/~/runs:batchGet", response_model=RunLogsResponse)
@v1.get("/activations/~/runs:batchGet", response_model=RunLogsResponse)
async def batch_get_connections_runs(
    session: AsyncSession = Depends(get_session),
    airflow_client=Depends(AirflowClient),
    final_connection_names: Annotated[list[str] | None, Query()] = None,
    connection_names: Annotated[list[str] | None, Query()] = None,
    page: int = 0,
    page_size: int = 20,
):
  """Retrieves run logs for all connections.
  
  Args:
    session: Postgres DB session dependency injection.
    airflow_client: Airflow Client dependency injection.
    activation_names: List of interested connection to retrieve logs from.
      Defaults to None, when logs from all connections are retrieved (legacy).
    connection_names: List of interested connections to retrieve logs from.
      Defaults to None, when logs from all connections are retrieved.
    page: Page number to be used by paginating clients. 
      Defaults to zero, when no offset is applied.
    page_size: Size of page to be used by paginating clients.
      Defaults to 20. 
  Returns:
    The RunLogsResponse object wrapped in an HTTP JSONResponse.
  """
  # define target connections
  connections = await get_connections(session)
  final_connection_names = final_connection_names or connection_names
  if not final_connection_names:  # if none passed, gets logs from all connections
    final_connection_names = [connection.name for connection in connections]
  else:  # if connection_names passed, filter connections object by name
    connections[:] = [a for a in connections if a.name in final_connection_names]

  connection_by_dag_id = {
      f"{connection.name}_dag": connection for connection in connections
  }

  runs_response = await airflow_client.list_dag_runs(
      connection_by_dag_id, limit=page_size, offset=page * page_size
  )

  return runs_response


app.mount("/api/v1", v1)
