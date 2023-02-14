"""API clients used by TIghtlock routes."""
import httpx
import datetime


_AIRFLOW_BASE_URL = "http://airflow-webserver:8080"


class AirflowClient:
  """Defines a base airflow client."""

  def __init__(self):
    self.base_url = f"{_AIRFLOW_BASE_URL}/api/v1"
    # TODO(b/267772197): Add functionality to store usn:password.
    self.auth = ("airflow", "airflow")

  async def trigger(self, dag_prefix: str):
    now_date = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    body = {
        "logical_date": now_date,
        "conf": {},
    }
    url = f"{self.base_url}/dags/{dag_prefix}_dag/dagRuns"
    async with httpx.AsyncClient() as client:
      return await client.post(url, json=body, auth=self.auth)