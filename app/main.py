from fastapi import FastAPI
from pydrill.client import PyDrill 
import os

app = FastAPI()
drill = PyDrill(host=os.environ.get('DRILL_HOSTNAME'), port=os.environ.get('DRILL_PORT'))

if not drill.is_active():
    raise Exception('Please run Drill first')

# Example route, exemplifying how to use Drill
@app.get("/")
def read_root():
    employees = drill.query('''
    SELECT * FROM cp.`employee.json` LIMIT 5
    ''')

    return list(employees)


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}