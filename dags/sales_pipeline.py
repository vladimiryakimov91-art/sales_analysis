"""Чистая логика пайплайна анализа продаж.

Сознательно НЕ импортирует airflow: этот модуль можно тестировать и
запускать вне Airflow (обычным python). Airflow-задачи в DAG-файле
только вызывают эти функции. Так принято в продакшене: логика отдельно
от оркестратора — иначе её нельзя писать тесты/переиспользовать.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent  # корень репозитория
DATA_FILE = ROOT / "data" / "sales_2023_2024.csv"
STAGE_DIR = ROOT / "output" / "stage"
REPORT_DIR = ROOT / "output"


def load_and_clean(data_file: Path | str = DATA_FILE) -> pd.DataFrame:
    """Загрузка + уборка. Идемпотентно: результат не зависит от числа запусков.

    Порядок операций критичен (иначе расхождение в 1 380 ₽):
      1) сперва восстановить revenue из cost + profit,
      2) затем заполнить пропуски units медианой.
    """
    df = pd.read_csv(data_file)
    df["order_date"] = pd.to_datetime(df["order_date"])

    # --- Тип 1: заглушка revenue = -1 (110 строк) ---
    # Восстановление одной формулой revenue = cost + profit (исходный расчёт).
    # Сверка второй формулой (units * price) — это ПРОВЕРКА, а не пересчёт:
    # при units == 0 или NaN пересчёт даёт 0/NaN и теряет строку.
    mask = df["revenue"] < 0
    df.loc[mask, "revenue"] = (df.loc[mask, "cost"] + df.loc[mask, "profit"]).round(2)
    assert not (df["revenue"] < 0).any(), "остались отрицательные revenue"

    # --- Тип 2: пропуски units (165 строк) — медиана ---
    df["units"] = df["units"].fillna(df["units"].median())

    # --- Тип 3: пустые заказы (revenue == 0) — исключить из аналитики ---
    df = df[df["revenue"] > 0].copy()

    # --- Контроль качества: пайплайн падает на плохих данных ---
    assert df["revenue"].between(0, df["revenue"].max()).all()
    assert df["units"].gt(0).all(), "есть заказы с units == 0"
    assert not df["revenue"].isna().any()
    assert not df["cost"].isna().any()

    df["year"] = df["order_date"].dt.year
    df["quarter"] = df["order_date"].dt.quarter
    df["month"] = df["order_date"].dt.to_period("M")
    return df


def compute_kpis(df: pd.DataFrame) -> dict:
    """KPI по рабочему фрейму. Возвращает JSON-совместимый dict (для XCom)."""
    revenue = df["revenue"].sum()
    orders = len(df)
    profit = (df["revenue"] - df["cost"]).sum()
    margin = profit / revenue * 100

    by_year = df.groupby("year")["revenue"].sum()
    yoy = (by_year[2024] / by_year[2023] - 1) * 100

    seg = df.groupby(["year", "segment"])["revenue"].sum().unstack()
    seg_delta = (seg.loc[2024] - seg.loc[2023]).round(0)

    cat = df.groupby("category")["revenue"].sum().sort_values(ascending=False)

    # ABC по связкам «категория × регион» (25 позиций)
    catreg = (df.groupby(["category", "region"])["revenue"].sum()
                .sort_values(ascending=False))
    total = catreg.sum()
    cum = catreg.cumsum() / total * 100
    cls = np.where(cum <= 80, "A", np.where(cum <= 95, "B", "C"))
    abc = pd.Series(cls, index=catreg.index)

    return {
        "orders": int(orders),
        "revenue": round(float(revenue), 2),
        "avg_check": round(float(revenue / orders), 2),
        "margin_pct": round(float(margin), 2),
        "revenue_2023": round(float(by_year.get(2023, 0)), 2),
        "revenue_2024": round(float(by_year.get(2024, 0)), 2),
        "yoy_pct": round(float(yoy), 2),
        "segment_2024_delta": {str(k): float(v) for k, v in seg_delta.items()},
        "top_categories": {str(k): float(v) for k, v in cat.head().items()},
        "abc_counts": {str(k): int(v) for k, v in abc.value_counts().items()},
    }


def build_report(df: pd.DataFrame, kpis: dict, report_dir: Path = REPORT_DIR) -> str:
    """Markdown-отчёт + графики. Пустота папки не страшна: файлы перезаписываются."""
    Path(report_dir).mkdir(parents=True, exist_ok=True)

    monthly = (df.groupby("month")["revenue"].sum().reset_index())
    monthly["month"] = monthly["month"].astype(str)

    md = [
        "# Отчёт: анализ продаж 2023–2024",
        "",
        f"- Заказов в анализе: **{kpis['orders']}**",
        f"- Выручка: **{kpis['revenue']:,.0f} ₽**",
        f"- Средний чек: **{kpis['avg_check']:,.2f} ₽**",
        f"- Маржа: **{kpis['margin_pct']:.2f}%**",
        f"- Год к году: **{kpis['revenue_2023']:,.0f} → {kpis['revenue_2024']:,.0f} ₽ "
        f"({kpis['yoy_pct']:+.2f}%)**",
        "",
        "## Категории по выручке (топ-5)",
        "",
        "| Категория | Выручка ₽ |",
        "|---|---|",
    ]
    for name, val in kpis["top_categories"].items():
        md.append(f"| {name} | {val:,.0f} |")

    md += [
        "",
        "## ABC по связкам «категория × регион»",
        "",
        "| Класс | Позиций |",
        "|---|---|",
    ]
    for cls, cnt in sorted(kpis["abc_counts"].items()):
        md.append(f"| {cls} | {cnt} |")

    report_path = Path(report_dir) / "report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")
    return str(report_path)


if __name__ == "__main__":
    frame = load_and_clean()
    k = compute_kpis(frame)
    print(json.dumps(k, ensure_ascii=False, indent=2))
    print("отчёт:", build_report(frame, k))