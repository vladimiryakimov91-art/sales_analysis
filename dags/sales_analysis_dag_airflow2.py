"""DAG: анализ продаж 2023–2024 — ВЕРСИЯ ДЛЯ AIRFLOW 2.x (2.9/2.10).

Зачем этот файл: сравнение синтаксиса с версией 3.x
(sales_analysis_dag.py). На собеседованиях чаще встречается именно 2.x,
и лучше знать оба варианта.

Отличия от 3.x в этом DAG:
  1. schedule_interval вместо schedule  (в 3.x schedule_interval УБРАН);
  2. в 2.x параметр schedule тоже существует (с 2.4), но в чужом коде
     вы чаще встретите именно schedule_interval — узнавайте оба;
  3. provide_context: в 2.x для PythonOperator нужен provide_context=True,
     чтобы передать контекст (ds, ti, dag_run...). В TaskFlow API
     контекст приходит автоматически. В 3.x provide_context убран совсем;
  4. XCom в 2.x: явный ti.xcom_push / ti.xcom_pull тоже живёт и широко
     встречается в старых DAG — в TaskFlow он скрыт возвратом функции.

ВНИМАНИЕ: файлы sales_analysis_dag.py (3.x) и этот (2.x) предназначены
для РАЗНЫХ версий Airflow — на конкретной установке поднимут оба DAG,
но рабочим будет только тот, чей синтаксис совпадает с установленной
версией. Для практики рассматривайте их как два варианта одного DAG.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from dags.sales_pipeline import compute_kpis, load_and_clean

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "sales_2023_2024.csv"
REPORT_DIR = BASE_DIR / "output"


@dag(
    dag_id="sales_analysis_pipeline_airflow2",
    description="Загрузка → уборка → метрики → отчёт (Airflow 2.x, TaskFlow API)",
    start_date=dt.datetime(2026, 1, 1),
    schedule_interval="0 6 * * 1",   # 2.x: классическое имя параметра
    catchup=False,
    tags=["pandas", "pipeline"],
    default_args={
        "owner": "vladimir",
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=5),
        "depends_on_past": False,
    },
    doc_md=__doc__,
)
def sales_analysis():
    """Пайплайн. Стрелки ниже — это и есть описание графа."""
    wait_for_data = FileSensor(
        task_id="wait_for_data",
        filepath=str(DATA_FILE),
        poke_interval=30,
        timeout=60 * 5,
        mode="poke",
    )

    @task
    def load_and_clean_task() -> dict:
        """Чистка данных + контроль качества (пайплайн падает на плохих)."""
        df = load_and_clean(DATA_FILE)
        return {"rows_clean": int(len(df))}

    @task
    def compute_kpis_task(load_info: dict) -> dict:
        """Метрики. Принимает результат предыдущей задачи (XCom)."""
        df = load_and_clean(DATA_FILE)
        return compute_kpis(df)

    @task
    def build_report_task(kpis: dict, report_dir: str = str(REPORT_DIR)) -> str:
        """Отчёт: markdown. Отдельная задача — падает только она."""
        df = load_and_clean(DATA_FILE)
        from dags.sales_pipeline import build_report
        return build_report(df, kpis, report_dir)

    # --- граф ---
    load_info = load_and_clean_task()
    kpis = compute_kpis_task(load_info)
    report = build_report_task(kpis)

    wait_for_data >> load_info


sales_analysis()