import json
import logging
import uuid
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def producer_func(init_date: str):
    logging.info(f"Kafka producer called with init_date: {init_date}")
    task_id = str(uuid.uuid4())
    value = {
        "task_id": task_id,
        "init_date": init_date,
    }
    return [
        (task_id.encode("utf-8"), json.dumps(value).encode("utf-8"))
    ]


@dag(
    "wb_commission_ingest",
    default_args=default_args,
    description="Сбор комиссий с wildberies",
    schedule="0 1 * * *",
    catchup=False,
    tags=["wildberries", "elt", "ingest"],
)
def wb_commision_ingest():
    logging.basicConfig(level=logging.INFO)

    kafka_notifier = ProduceToTopicOperator(
        task_id="start_data_pipine_produce_task_to_kafka",
        topic="ingest-commission-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"init_date": "{{ ts }}"},

    )

    kafka_notifier


wb_commision_ingest()
