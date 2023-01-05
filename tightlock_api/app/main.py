from fastapi import FastAPI
from pydrill.client import PyDrill 
import os
import databases
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

app = FastAPI()
v1 = FastAPI()

drill = PyDrill(host=os.environ.get('DRILL_HOSTNAME'), port=os.environ.get('DRILL_PORT'))

# follow example here: https://fastapi.tiangolo.com/advanced/async-sql-databases/
config_conn= os.environ.get('CONFIG_DB_CONN')
config_db = databases.Database(config_conn)

if not drill.is_active():
    raise Exception('Please run Drill first')


@app.on_event("startup")
async def startup():
    await config_db.connect()


@app.on_event("shutdown")
async def shutdown():
    await config_db.disconnect()


@v1.post("/connect")
def connect():
    pass

    
@v1.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}


app.mount("/api/v1", v1)