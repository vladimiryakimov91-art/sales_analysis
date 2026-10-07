"""DAG: анализ продаж 2023–2024 (загрузка → уборка → метрики → отчёт).

Airflow 3.x, TaskFlow API (@dag / @task). Версии 2.x: тот же код с
schedule_interval и без агрегированных декораторов в некоторых местах.

Что демонстрирует (вопросы с собеседований):
  - TaskFlow: данные между задачами через XCom (возврат функции);
  - schedule как строка/объект расписания;
  - catchup=False: не гоняем историю при старте;
  - retries + retry_delay: перезапуск задачи при сбое;
  - сенсор: ждём появления файла данных (вместо хардкода графика);
  - логика DAG отделена от логики расчёта (sales_pipeline.py).
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
    dag_id="sales_analysis_pipeline",
    description="Загрузка → уборка → метрики → отчёт (пет-проект анализа продаж)",
    start_date=dt.datetime(2026, 1, 1),
    schedule="0 6 * * 1",          # понедельник в 06:00 (cron)
    catchup=False,                  # не запускать пропущенные даты задним числом
    tags=["pandas", "pipeline"],
    default_args={
        "owner": "vladimir",
        "retries": 2,               # 2 повторные попытки перед задачей в fail
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
        poke_interval=30,           # проверка каждые 30 сек
        timeout=60 * 5,             # сдаться через 5 минут
        mode="poke",
    )

    @task
    def load_and_clean_task() -> dict:
        """Чистка данных + контроль качества (пайплайн падает на плохих)."""
        df = load_and_clean(DATA_FILE)
        # Возвращаем в XCom не DataFrame целиком, а итоговые счётчики:
        # XCom хранит сериализуемое, DataFrame туда класть не стоит.
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
        path = build_report(df, kpis, report_dir)
        return path

    # --- граф ---
    load_info = load_and_clean_task()
    kpis = compute_kpis_task(load_info)
    report = build_report_task(kpis)

    # зависимость: сенсор → первая задача пайплайна
    wait_for_data >> load_info


sales_analysis()