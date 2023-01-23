"""Entrypoint for FastAPI application."""
import contextlib
import json
from typing import List
import os


from db import get_session
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from models import Activation
from models import Config
from models import DrillConnection
from pydrill.client import PyDrill
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession



class DrillConnectionException(Exception):
  "Raised when there is a problem with Drill Connection creation"
  pass

app = FastAPI()
v1 = FastAPI()

drill = PyDrill(host=os.environ.get("DRILL_HOSTNAME"),
                port=os.environ.get("DRILL_PORT"))

if not drill.is_active():
  raise Exception("Please run Drill first")

# TODO(b/265713009): Update drill connections each time a new Config is created
async def update_drill_connections(conns: List[DrillConnection]):
  base_config_names = ['cp', 'dfs', 'http', 's3', 'cassandra', 'mongo', 'hbase', 'hive', 'rdbms']

  for conn in conns:
    if conn.name in base_config_names:
      raise DrillConnectionException(f"Connection name {conn.name} not allowed. Please provide another name.")
    storage = None
    match conn.type:
      case "bigquery":
        storage_result = drill.storage_detail('http')
        storage = storage_result.data
      case _:
        raise DrillConnectionException(f"Connection type {conn.type} unknown.")
    storage["config"] = conn.config
    drill.storage_update(conn.name, conn.config)
  

@app.on_event("startup")
async def create_initial_config():
  f = open("base_config.json")
  data = json.load(f)
  get_session_wrapper = contextlib.asynccontextmanager(get_session)
  async with get_session_wrapper() as session:
    try:
      await create_config(Config(label="Initial Config",
                                 value=data),
                          session=session)
    except HTTPException:
      pass  # Ignore workers trying to recreate initial config


@app.on_event("shutdown")
async def shutdown():
  pass


# TODO(b/264568702)
@v1.post("/connect")
def connect():
  pass


# TODO(b/264570105)
@v1.post("/activations/{activation_id}:trigger")
async def trigger_activation(activation_id: int):
  return activation_id


@v1.get("/configs", response_model=list[Config])
async def get_configs(session: AsyncSession = Depends(get_session)):
  result = await session.execute(select(Config))
  configs = result.scalars().all()
  return [Config(create_date=config.create_date, label=config.label,
                 value=config.value, id=config.id
                ) for config in configs]


@v1.get("/configs:getLatest", response_model=Config)
async def get_latest_config(session: AsyncSession = Depends(get_session)):
  statement = select(Config).order_by(Config.timestamp.desc()).limit(1)
  result = await session.execute(statement)
  latest_config = result.one()
  return latest_config
  

# TODO(b/264569609)
@v1.get("/configs/{config_id}", response_model=Config)
async def get_config(config_id: int, session: AsyncSession = Depends(get_session)):
  # description: get a config with the provided id
  return Config(id=config_id)


@v1.post("/configs", response_model=Config)
async def create_config(config: Config,
                        session: AsyncSession = Depends(get_session)):
  session.add(config)
  try:
    connections = [DrillConnection(name=conn["name"],
                                   type=conn["type"],
                                   config=conn["config"]) for conn in config.value["external_connections"]]
    await update_drill_connections(connections)
    # commiting session should be the last operation of the creation sequence
    await session.commit()
  except DrillConnectionException as exc:  # Raised when Drill Connection is invalid.
    raise HTTPException(status_code=422,
                         detail=str(exc)) from exc
  except IntegrityError as exc:  # Raised when label uniqueness is violated.
    raise HTTPException(status_code=409,
                        detail=f"Config label {config.label} already exists."
                       ) from exc
  return config


# TODO(b/264569406)
@v1.get("/activations", response_model=list[Activation])
async def get_activations(session: AsyncSession = Depends(get_session)):
  # description: query latest config and query activations field from config json
  return []


app.mount("/api/v1", v1)