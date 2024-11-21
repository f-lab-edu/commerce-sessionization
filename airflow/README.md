### Installation

1. 환경 설정
    ``` bash
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
2. `docker compose up airflow-init`
3. `docker compose up -d`
4. `Airflow` - `Admin` - `Connections`

   Google Cloud Connection 정보 입력

   e.g. Keyfile JSON

### 출처

- https://airflow.apache.org/docs/apache-airflow/2.9.1/howto/docker-compose/index.html
- https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
