import json
import logging
import uuid
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.sdk import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def producer_func(init_date: str):
    logging.info(f"Kafka producer called with init_date: {init_date}")
    task_id = str(uuid.uuid4())

    wb_token = Variable.get("WB_API_TOKEN")  # Make sure this variable exists in Airflow

    value = {
        "task_id": task_id,
        "init_date": init_date,
        "wb_token": wb_token,
    }
    return [
        (task_id.encode("utf-8"), json.dumps(value).encode("utf-8"))
    ]


@dag(
    "wb_ingest",
    default_args=default_args,
    description="Запуск инжеста данных из вб",
    schedule="0 1 * * *",
    catchup=False,
    tags=["wildberries", "elt", "ingest"],
)
def wb_commision_ingest():
    logging.basicConfig(level=logging.INFO)

    commissions = ProduceToTopicOperator(
        task_id="send_commissions_ingest_task_to_topic",
        topic="ingest-commissions-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"init_date": "{{ ts }}"},

    )
    commissions


wb_commision_ingest()
