# Databricks notebook source
# MAGIC %md
# MAGIC # Duplications - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC # Statistics and Scenarios

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sf = "dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips/2022-08-23.parquet"
sf_buffer_strips = "dbfs:/mnt/lab/unrestricted/DSMT/gis/parcel_buffer_strips/2022-08-24.parquet"

# COMMAND ----------

df_parcel = spark.read.parquet(sf).select(
    "id_business",
    "id_parcel",
    "parcel_area",
    "hedge_length",
    "water_length",
)
df_parcel.write.parquet(sf_buffer_strips, mode="overwrite")
df_parcel = df_parcel.toPandas()

display(df_parcel)

df_business = df_parcel.assign(parcel_count=1).groupby("id_business").sum().reset_index().replace(0, np.nan)

display(df_business)

# COMMAND ----------

# MAGIC %md ### Statistics

# COMMAND ----------

print(f"Total Parcels:  {df_parcel.shape[0]:,.0f}")
print(f'Total Hectarage:  {df_parcel["parcel_area"].sum()/1e4:,.0f}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distributions

# COMMAND ----------

fig, (ax0, ax1, ax2) = plt.subplots(3, figsize=(6, 8))


ds = df_business["hedge_length"]
sns.histplot(ds, ax=ax0, stat="density").set(
    title="Hedgerow per Business",
    xlabel="length per hectare  (m / business)",
    xlim=[0, ds.quantile(0.95)],
)
ax0.axvline(ds.mean(), color="k", linestyle="dashed")
ax0.axvline(ds.median(), color="k", linestyle="dotted")


ds = df_parcel["hedge_length"]
sns.histplot(ds, ax=ax1, stat="density").set(
    title="Hedgerow per Parcel",
    xlabel="length per hectare  (m / parcel)",
    xlim=[0, ds.quantile(0.95)],
)
ax1.axvline(ds.mean(), color="k", linestyle="dashed")
ax1.axvline(ds.median(), color="k", linestyle="dotted")


ds = df_parcel["hedge_length"] / df_parcel["parcel_area"] * 1e4
sns.histplot(ds, ax=ax2, stat="density").set(title="Hedgerow per Hectare", xlabel="length per hectare  (m / ha)", xlim=[0, ds.quantile(0.95)])
l0 = ax2.axvline(ds.mean(), color="k", linestyle="dashed", label="mean")
l1 = ax2.axvline(ds.median(), color="k", linestyle="dotted", label="median")


fig.legend(handles=[l0, l1], loc="right")

fig.tight_layout()

# COMMAND ----------

fig, (ax0, ax1, ax2) = plt.subplots(3, figsize=(6, 8))


ds = df_business["water_length"]
sns.histplot(ds, ax=ax0, stat="density").set(
    title="Waterbody per Business",
    xlabel="length per hectare  (m / business)",
    xlim=[0, ds.quantile(0.95)],
)
ax0.axvline(ds.mean(), color="k", linestyle="dashed")
ax0.axvline(ds.median(), color="k", linestyle="dotted")


ds = df_parcel["water_length"]
sns.histplot(ds, ax=ax1, stat="density").set(
    title="Waterbody per Parcel",
    xlabel="length per hectare  (m / parcel)",
    xlim=[0, ds.quantile(0.95)],
)
ax1.axvline(ds.mean(), color="k", linestyle="dashed")
ax1.axvline(ds.median(), color="k", linestyle="dotted")


ds = df_parcel["water_length"] / df_parcel["parcel_area"] * 1e4
sns.histplot(ds, ax=ax2, stat="density").set(
    title="Waterbody per Hectare",
    xlabel="length per hectare  (m / ha)",
    xlim=[0, ds.quantile(0.95)],
)
l0 = ax2.axvline(ds.mean(), color="k", linestyle="dashed", label="mean")
l1 = ax2.axvline(ds.median(), color="k", linestyle="dotted", label="median")


fig.legend(handles=[l0, l1], loc="right")

fig.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenarios

# COMMAND ----------

# Scenario Setup
pay = 21
l = 100  # noqa:E741

df_parcel_hedge = df_parcel[df_parcel["hedge_length"].notna()]
df_business_hedge = df_business[df_business["hedge_length"].notna()]

df = pd.DataFrame(columns=["Payment", "Average", "Units", "Eligible Units", "Uptake", "Admin Ratio"])


# Scenraio 1
lengths = df_business_hedge["hedge_length"]
elig = np.floor(lengths / l).sum()
avg = l
total = lengths.sum()

cost = avg * elig * pay / l
uptake = avg * elig / total


def admin_ratio(c, u):
    return (c / u - cost / uptake) / elig


df = df.append(
    {
        "Payment": cost,
        "Average": avg / uptake,
        "Units": "",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 2
lengths = df_parcel_hedge["hedge_length"]
elig = df_parcel_hedge.shape[0]

avg = lengths.mean()
are_in = lengths < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg,
        "Units": "parcel",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 3
avg = lengths.median()
are_in = lengths < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg,
        "Units": "parcel",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 4
lengths, areas = df_business_hedge["hedge_length"], df_business_hedge["parcel_area"]
lengths_per_area = lengths / areas
elig = areas.sum()

avg = lengths_per_area.mean()
are_in = lengths_per_area < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg * 1e4,
        "Units": "hectare",
        "Eligible Units": elig / 1e4,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 5
avg = lengths_per_area.median()
are_in = lengths_per_area < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg * 1e4,
        "Units": "hectare",
        "Eligible Units": elig / 1e4,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


display(df)

# COMMAND ----------

# Scenario Setup
pay = 29
l = 100  # noqa:E741

df_parcel_water = df_parcel[df_parcel["water_length"].notna()]
df_business_water = df_business[df_business["water_length"].notna()]

df = pd.DataFrame(columns=["Payment", "Average", "Units", "Eligible Units", "Uptake", "Admin Ratio"])


# Scenraio 1
lengths = df_business_water["water_length"]
elig = np.floor(lengths / l).sum()
avg = l
total = lengths.sum()

cost = avg * elig * pay / l
uptake = avg * elig / total


def admin_ratio(c, u):
    return (c / u - cost / uptake) / elig


df = df.append(
    {
        "Payment": cost,
        "Average": avg / uptake,
        "Units": "",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 2
lengths = df_parcel_water["water_length"]
elig = df_parcel_water.shape[0]

avg = lengths.mean()
are_in = lengths < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg,
        "Units": "parcel",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 3
avg = lengths.median()
are_in = lengths < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg,
        "Units": "parcel",
        "Eligible Units": elig,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 4
lengths, areas = df_business_water["water_length"], df_business_water["parcel_area"]
lengths_per_area = lengths / areas
elig = areas.sum()

avg = lengths_per_area.mean()
are_in = lengths_per_area < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg * 1e4,
        "Units": "hectare",
        "Eligible Units": elig / 1e4,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


# Scenario 5
avg = lengths_per_area.median()
are_in = lengths_per_area < avg
cost = are_in.mean() * avg * elig * pay / l
uptake = lengths[are_in].sum() / lengths.sum()

df = df.append(
    {
        "Payment": cost,
        "Average": avg * 1e4,
        "Units": "hectare",
        "Eligible Units": elig / 1e4,
        "Uptake": uptake,
        "Admin Ratio": admin_ratio(cost, uptake),
    },
    ignore_index=True,
)


display(df)
