"""Entrypoint for FastAPI application."""
import contextlib
import json
import os

from db import get_session
from fastapi import Depends
from fastapi import FastAPI
from models import Activation
from models import Config
from pydrill.client import PyDrill
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession


app = FastAPI()
v1 = FastAPI()

drill = PyDrill(host=os.environ.get("DRILL_HOSTNAME"),
                port=os.environ.get("DRILL_PORT"))

if not drill.is_active():
  raise Exception("Please run Drill first")


@app.on_event("startup")
async def startup():
  f = open("base_config.json")
  data = json.load(f)
  get_session_wrapper = contextlib.asynccontextmanager(get_session)
  async with get_session_wrapper() as session:
    try:
      await create_config(Config(label="Initial Config", value=data), session=session)
    except IntegrityError:
      pass  # ignore if the initial config was already created by another worker



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
  return [Config(timestamp=config.timestamp, label=config.label,
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
  await session.commit()
  return config


# TODO(b/264569406)
@v1.get("/activations", response_model=list[Activation])
async def get_activations(session: AsyncSession = Depends(get_session)):
  # description: query latest config and query activations field from config json
  return []


app.mount("/api/v1", v1)