from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.apache.drill.hooks.drill import DrillHook

from activations import ga4mp

def get_from_config_list(config_list, target_name):
  result = [target for target in config_list if target["name"] == target_name]
  if len(result) > 0:
    return result[0]
  raise LookupError(f"{target_name} not found in {config_list}")

def get_latest_config():
  sql_stmt = "SELECT value FROM Config ORDER BY create_date DESC LIMIT 1"
  pg_hook = PostgresHook(
    postgres_conn_id='tightlock_config',
  )
  pg_conn = pg_hook.get_conn()
  cursor = pg_conn.cursor()
  cursor.execute(sql_stmt)
  return cursor.fetchone()

# User Drill hook
latest_config = get_latest_config()[0]

for activation in latest_config["activations"]:
  dag_id = f"{activation['name']}_dag"
  schedule_interval = activation["schedule"] if activation["schedule"] else None

  activation_source = get_from_config_list(latest_config["sources"], activation["source_name"])
  activation_destination = get_from_config_list(latest_config["destinations"], activation["destination_name"])

  @dag(dag_id=dag_id, start_date=datetime.now(), schedule_interval=schedule_interval)
  def dynamic_generated_dag():
    @task
    def get_drill_data(query):
        drill_conn = DrillHook().get_conn()
        cursor = drill_conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @task
    def activate(data):
      # TODO(b/265710315) Refactor code below once activation logic is more generic
      try:
        config_type = activation_destination["config"]["type"]
        if config_type == "GA4MP":
          ga4mp.activate(data)
        else:
          raise NotImplementedError(f"Unknow type: {config_type}")
      except KeyError as exc:
        raise LookupError(f"{activation['destination_name']} must have a config type.") from exc
    
    query = ga4mp.get_query(activation_source["location"])
    activate(get_drill_data(query))

  dynamic_generated_dag()