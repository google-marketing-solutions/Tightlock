"""Entrypoint for FastAPI application."""
import contextlib
import json

from clients import AirflowClient
from db import get_session
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import Response
from models import Activation
from models import Config
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

# Creates base app and v1 API objects
app = FastAPI()
v1 = FastAPI()


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


# TODO(b/264568702)
@v1.post("/connect")
async def connect():
  """Validates API connection with a client."""
  pass


@v1.post("/activations/{activation_name}:trigger")
async def trigger_activation(
    activation_name: str, airflow_client=Depends(AirflowClient)
):
  """Triggers an activation identified by name."""
  response = await airflow_client.trigger(activation_name)
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
  row = result.one()
  return Config(
      create_date=row[0].create_date,
      label=row[0].label,
      value=row[0].value,
      id=row[0].id,
  )


@v1.get("/configs/{config_id}", response_model=Config)
async def get_config(config_id: int, session: AsyncSession = Depends(get_session)):
  """Retrieves a config with the provided id."""
  statement = select(Config).where(Config.id == config_id)
  config = await session.execute(statement)
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
  return [
      Activation(
          name=a["name"],
          source_name=a["source_name"],
          destination_name=a["destination_name"],
          schedule=a["schedule"],
      )
      for a in latest_config.value["activations"]
  ]


app.mount("/api/v1", v1)
