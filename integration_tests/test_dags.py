"""Integration tests for airflow-webserver container."""

from urllib import parse


def test_dag_import_errors(helpers):
  """Verifies if there are no DAG import errors."""
  # TODO(b/267772197): Add functionality to store usn:password.
  request_session, api_url = helpers.get_container_client(
      "airflow-webserver", ("airflow", "airflow"))
  import_errors = request_session.get(
      parse.urljoin(api_url,
                    "api/v1/importErrors")).json()
  if import_errors["total_entries"] > 0:
    for error in import_errors["import_errors"]:
      print(f"Import Error for DAG {error['filename']}: {error['stack_trace']}")
  assert import_errors["total_entries"] == 0
