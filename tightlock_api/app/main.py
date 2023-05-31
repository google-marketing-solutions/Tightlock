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
limitations under the License."""

"""Entrypoint for FastAPI application."""
import contextlib
import json
from typing import Annotated, Any

from clients import AirflowClient
from db import get_session
from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import Response
from models import Activation, Config, ConfigValue, RunLog, ValidationResult
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
      await create_config(Config(label="Initial Config", value=data), session=session)
    except HTTPException:
      pass  # Ignore workers trying to recreate initial config


@v1.post("/connect")
async def connect():
  """Validates API connection with a client."""
  return Response(status_code=status.HTTP_200_OK)


@v1.post("/activations/{activation_name}:trigger")
async def trigger_activation(
    activation_name: str,
    dry_run: bool = Body(..., embed=True),
    airflow_client=Depends(AirflowClient),
):
  """Triggers an activation identified by name."""
  trigger_conf = {"dry_run": dry_run}
  response = await airflow_client.trigger(activation_name, conf=trigger_conf)
  return Response(status_code=response.status_code)


@v1.get("/configs", response_model=list[Config])
async def get_configs(session: AsyncSession = Depends(get_session)):
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
  """Retrieves the most recent config."""
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
async def get_config(config_id: int, session: AsyncSession = Depends(get_session)):
  """Retrieves a config with the provided id."""
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
async def create_config(config: Config, session: AsyncSession = Depends(get_session)):
  """Creates a new config using the provided config object."""
  session.add(config)
  try:
    await session.commit()
  except IntegrityError as exc:  # Raised when label uniqueness is violated.
    raise HTTPException(
        status_code=409, detail=f"Config label {config.label} already exists."
    ) from exc
  return config


@v1.get("/activations", response_model=list[Activation])
async def get_activations(session: AsyncSession = Depends(get_session)):
  """Queries latest config and query activations field from config json."""
  latest_config = await get_latest_config(session=session)
  activations = [Activation(**a) for a in latest_config.value["activations"]]
  return activations


@v1.get("/schemas", response_model=dict[str, Any])
async def get_schemas():
  """Retrieves available schemas for Activations."""
  # TODO(b/270748315): Implement actual call to schemas DAG once available.
  f = open("schemas_sample.json")
  data = json.load(f)
  return data


@v1.post("/sources/{source_name}:validate", response_model=ValidationResult)
async def validate_source(
    source_name: str,
    source_config: ConfigValue,
    airflow_client=Depends(AirflowClient),
):
  response = await airflow_client.validate_source(source_name, source_config.value)
  return response


@v1.post("/destinations/{destination_name}:validate", response_model=ValidationResult)
async def validate_destination(
    destination_name: str,
    destination_config: ConfigValue,
    airflow_client=Depends(AirflowClient),
):
  response = await airflow_client.validate_destination(
      destination_name, destination_config.value
  )
  return response


@v1.get("/activations/~/runs:batchGet", response_model=list[RunLog])
async def batch_get_activations_runs(
    session: AsyncSession = Depends(get_session),
    airflow_client=Depends(AirflowClient),
    activation_names: Annotated[list[str] | None, Query()] = None,
    page: int = 0,
    page_size: int = 20,
):
  # define target activations
  activations = await get_activations(session)
  if not activation_names:  # if none passed, gets logs from all activations
    activation_names = [activation.name for activation in activations]
  else:  # if activation_names passed, filter activations object by name
    activations[:] = [a for a in activations if a.name in activation_names]

  activation_by_dag_id = {
      f"{activation.name}_dag": activation for activation in activations
  }

  runs_response = await airflow_client.list_dag_runs(
      activation_by_dag_id, limit=page_size, offset=page * page_size
  )

  return runs_response


app.mount("/api/v1", v1)
