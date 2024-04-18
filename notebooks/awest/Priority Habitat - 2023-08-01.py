# Databricks notebook source
# MAGIC %md
# MAGIC Key areas of interest would be as follows, with further detail in the chain below.
# MAGIC
# MAGIC Average parcel/field size of Priority habitat (by type)
# MAGIC Average number of parcels/fields of Priority habitat (by type) per SBI.
# MAGIC Average area of priority habitat (by type) per SBI

# COMMAND ----------

import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

f_wfm = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_parcels.feather"
f_ph = "/dbfs/mnt/lab/unrestricted/elm/elmo/priority_habitats/output.feather"

# COMMAND ----------

df_wfm = pd.read_feather(f_wfm)[["id_business", "id_parcel", "ha_parcel_uaa"]]
df_ph = pd.read_feather(f_ph)[["id_parcel", "Main_Habit", "confidence", "proportion"]]
df = df_wfm.merge(df_ph).assign(
    ha=lambda df: (df["ha_parcel_uaa"] * (df["proportion"] > 0.1)).replace(0, np.NAN),
    ha_ph=lambda df: df["ha_parcel_uaa"] * df["proportion"],
    count=1,
    log_ha=lambda df: np.log10(df["ha"]),
)

df_ph = (
    df.groupby(["id_business", "Main_Habit"])[["ha", "count", "ha_ph"]]
    .sum()
    .reset_index()
    .assign(
        log_count=lambda df: np.log10(df["count"]),
        log_ha_ph=lambda df: np.log10(df["ha_ph"]),
    )
)


# COMMAND ----------

df_means = pd.DataFrame(
    {
        "Mean Parcel Area": df.groupby("Main_Habit")["ha"].mean(),
        "Mean Parcel Count per Business": df_ph.groupby("Main_Habit")["count"].mean(),
        "Mean Business Habitat Area": df_ph.groupby("Main_Habit")["ha_ph"].mean(),
    },
).reset_index()

display(df_means)

# COMMAND ----------


def kdeplots(df, x, y, rot):
    # https://seaborn.pydata.org/examples/kde_ridgeplot.html
    warnings.filterwarnings("ignore", "", UserWarning)
    sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
    pal = sns.cubehelix_palette(df[y].nunique(), rot=rot, dark=0.25, light=0.75)
    g = sns.FacetGrid(df, row=y, hue=y, aspect=15, height=0.5, palette=pal)
    g.map(sns.kdeplot, x, bw_adjust=0.5, clip_on=False, fill=True, alpha=1, linewidth=1.5)
    g.map(sns.kdeplot, x, clip_on=False, color="w", lw=2, bw_adjust=0.5)
    g.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)

    def label(x, color, label):
        ax = plt.gca()
        ax.text(
            0,
            0.2,
            label,
            fontweight="bold",
            color="k",
            ha="left",
            va="center",
            transform=ax.transAxes,
        )

    g.map(label, x)
    g.figure.subplots_adjust(hspace=-0.25)
    g.set_titles("")
    g.set(yticks=[], ylabel="")
    g.despine(bottom=True, left=True)


kdeplots(df, "log_ha", "Main_Habit", -0.5)
kdeplots(df_ph, "log_count", "Main_Habit", 0)
kdeplots(df_ph, "log_ha_ph", "Main_Habit", 0.5)

# COMMAND ----------


def histplots(df, x, y):
    fig, axs = plt.subplots(5, 6, figsize=(16, 9))
    axs = [ax for ax0 in axs for ax in ax0]
    for ph, ax in zip(df[y].unique(), axs):
        df0 = df.query(f'{y} == "{ph}"')
        ax.hist(df0[x], bins=20)
        ax.set_title(ph)
    fig.tight_layout()


histplots(df, "ha", "Main_Habit")
histplots(df_ph, "count", "Main_Habit")
histplots(df_ph, "ha_ph", "Main_Habit")
