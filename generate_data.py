import pandas as pd
import numpy as np

np.random.seed(2024)

n = 11000
dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
categories = ["Дом и сад", "Офис", "Канцелярия", "Техника", "Мебель"]
regions = ["Москва", "Санкт-Петербург", "Екатеринбург", "Новосибирск", "Самара"]
segments = ["Потребитель", "Корпоратив", "Мелкий опт"]

rows = []
for _ in range(n):
    date = np.random.choice(dates)
    cat = np.random.choice(categories, p=[0.28, 0.22, 0.20, 0.18, 0.12])
    region = np.random.choice(regions)
    segment = np.random.choice(segments, p=[0.55, 0.30, 0.15])
    units = int(np.random.lognormal(mean=2.2, sigma=0.9))
    price = round(np.random.lognormal(mean=6.0, sigma=0.8), 2)
    rows.append([date, cat, region, segment, units, price])

df = pd.DataFrame(rows, columns=["order_date", "category", "region", "segment", "units", "price"])

df["order_id"] = range(1, len(df) + 1)
df["revenue"] = (df["units"] * df["price"]).round(2)
df["cost"] = (df["revenue"] * np.random.uniform(0.55, 0.80, len(df))).round(2)
df["profit"] = (df["revenue"] - df["cost"]).round(2)

df = df[["order_id", "order_date", "category", "region", "segment", "units", "price", "revenue", "cost", "profit"]]

df.loc[df.sample(frac=0.015).index, "units"] = np.nan
df.loc[df.sample(frac=0.010).index, "revenue"] = -1

from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)

df.to_csv(DATA_DIR / "sales_2023_2024.csv", index=False, encoding="utf-8-sig")
print("Rows:", len(df))
print(df.head(3).to_string())
print("nulls:", df.isna().sum().sum(), "| revenue == -1:", (df.revenue < 0).sum(),
      "| revenue == 0:", (df.revenue == 0).sum())