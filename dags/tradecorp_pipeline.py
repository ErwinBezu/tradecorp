from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount

PROJECT_PATH = "//c/Users/Utilisateur/Documents/formation/tradecorp"

MOUNTS = [
    Mount(
        source=f"{PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind"
    ),
    Mount(
        source=f"{PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind"
    ),
    Mount(
        source=f"{PROJECT_PATH}/.env",
        target="/home/jovyan/.env",
        type="bind"
    )
]

default_args = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args= default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval= "0 6 * * *",
    catchup=False,
    tags=["tradecorp", "etl"],
) as dag:
    attendre_fichier = FileSensor(
        task_id="attendre_go",
        filepath="/opt/airflow/data/trigger/go.txt",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    t0 = DockerOperator(
        task_id="fetch_exchange_rates",
        image="tradecorp-spark",
        command="python /home/jovyan/src/fetch_exchange_rates.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
    )
    t1 = DockerOperator(
        task_id="reader",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/reader.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
    )

    t2 = DockerOperator(
        task_id="transformer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/transformer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
    )

    t3 = DockerOperator(
        task_id="writer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/writer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
    )


    attendre_fichier >> t0 >> t1 >> t2 >> t3