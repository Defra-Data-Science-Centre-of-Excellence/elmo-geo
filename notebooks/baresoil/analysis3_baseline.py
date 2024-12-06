# Databricks notebook source
# MAGIC %md # Baseline Analysis
# MAGIC We would like to see what the baseline should be for each of the arable and improved grassland SFI actions. From an excel () we find that the baseline
# MAGIC is set at 65% and 77% respectively for arable and improved grassllands currently. This was calculated from older data. The action requires 70% and 90%
# MAGIC greenery respectively for arable and imprvoed grassland actions.
# MAGIC
# MAGIC In this notebook, we will:
# MAGIC - Locate the data for several years of both types of parcels.
# MAGIC - Analyse and find averages for each of them
# MAGIC - Plot visualisations with regards to year and ..... for each of the types of parcels


# COMMAND ----------

# from matplotlib.ticker import PercentFormatter
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import seaborn as sns
from pyspark.sql.functions import lit

# from matplotlib.dates import DateFormatter
from elmo_geo.rs.sentinel import sentinel_years
from elmo_geo.utils.dbr import spark

# COMMAND ----------

years = sentinel_years.copy()
paths = [f"/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet" for year in years]
paths_iter = iter(paths)
df = spark.read.parquet(next(paths_iter)).withColumn("year", lit(years[0]))

for f, y in zip(paths_iter, years[1:]):
    df = df.union(spark.read.parquet(f).withColumn("year", lit(y)))

df = df.toPandas()

# COMMAND ----------

# Import WFM data for info on fields and joining onto data from above
wfm_path = "/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/parcels_v2.parquet"
cols = spark.read.parquet(wfm_path).columns
cols = [col for col in cols if col in ("id_business", "id_parcel") or col.startswith("ha_")]
wfm = spark.read.parquet(wfm_path).select(*cols).repartition(100).toPandas()
wfm = wfm.set_index("id_parcel").join(df.set_index("id_parcel"), how="right")
wfm = wfm.dropna()

wfm.loc[:, ["year", "bare_soil_percent"]].groupby(by="year").count()

# COMMAND ----------


def add_bool_col(df, col_list, new_col_name):
    """Adds a boolean column to dataframe."""
    df[new_col_name] = df[filter(col_list, df.columns)].sum(axis=1) > 0
    return df


# arable soil column
def predicate(col):
    return col.startswith("ha_arable_")


wfm = add_bool_col(wfm, predicate, "arable")


# improved grassland column
def predicate(col):
    return col in [
        "ha_grassland_temporary_improved_grades_1_2",
        "ha_grassland_temporary_improved_grades_3_plus",
        "ha_grassland_improved_disadvantaged",
        # "ha_arable_grassland_temporary",
    ]


wfm = add_bool_col(wfm, predicate, "improved_grassland")
print(wfm.shape)

wfm.head()

# COMMAND ----------


def mean_df(df, information_type):
    """
    Creates the dataframes for the means of the bare soil percentage per year
    Parameters:
    - df: the dataframe you will be getting the means from
    - information_type: ['arable','grassland','total']
    Returns
    - DF consisting of only year and mean of bare soil percentage
    """
    cols = ["bare_soil_percent", "tile", "year"]
    if information_type == "total":
        df_new = df.loc[:, cols]
    else:
        df_new = df.loc[df[information_type], cols]
    df_new = df_new.pivot(columns="year", values="bare_soil_percent")

    df_new = pd.DataFrame(df_new.mean(), columns=[f"bare_soil_{information_type}"])
    df_new["year"] = pd.to_datetime(df_new.index, format="%Y")
    return df_new


arable_means = mean_df(wfm, "arable")
grass_means = mean_df(wfm, "improved_grassland")
total_means = mean_df(wfm, "total")


# COMMAND ----------


def format_winter(date: pd.Timestamp) -> str:
    """Format a winter date to a string like `2023-24`"""
    if date.month < 3:
        year_st = date.year - 1
    elif date.month > 10:
        year_st = date.year
    else:
        raise ValueError(f"Received date ({date}) is not in the winter months (November-Feb)")
    return f"{year_st}-{int(str(year_st)[-2:])+1}"


def mean_trends(df1, df2, df3):
    """
    Creating a ftrnd line graph to find the aversges through the years of each type of field

    Parameters:
    - df1 - dataframe with arable field means that has gone through function means_df. Each field type should have its own dataframe
    - df2: dataframe with improved grasssland field means that has gone through function means_df. Each field type should have its own dataframe
    - df3: dataframe with all field means that has gone through function means_df. Each field type should have its own dataframe

    Returns:
    - Figure
    """
    years = [i for i in df1["year"]]
    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context(font_scale=1.3)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.grid(visible=False, which="both", axis="x")
    for d in range(3):
        ax.plot(
            df3.year,
            df3[df3.columns[0]],
            marker="o",
            mec="white",
            linewidth=0.6,
            color="#77BC1F",
            markersize=10,
        )
        ax.plot(
            df1.year,
            df1[df1.columns[0]],
            marker="o",
            mec="white",
            linewidth=0.6,
            color="#6D3075",
            markersize=10,
        )
        ax.plot(
            df2.year,
            df2[df2.columns[0]],
            marker="o",
            mec="white",
            linewidth=0.6,
            color="#D9296E",
            markersize=10,
        )
    labels = [
        "All fields",  # df3.columns[0][10:].capitalize()
        f"{df1.columns[0][10:].capitalize()} & horticultural",
        df2.columns[0][10:].replace("_", " ").capitalize(),
    ]
    ax.legend(
        labels=labels,
        loc="upper center",
    )
    ax.set_xlabel("Winter")
    ax.set_xlim(years[0] + pd.DateOffset(months=-2), years[2] + pd.DateOffset(months=2))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: format_winter(mdates.num2date(x))))
    ax.set_ylim(0, 0.2)
    ax.set_ylabel("Bare soil percentage")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))
    fig.suptitle(
        t="Mean bare soil over winter in fields across England",
        x=0.02,
        ha="left",
        fontsize="x-large",
    )
    #     for d in years:
    #         ax.axvline(d, alpha=0.15)
    sns.despine(top=True, right=True, left=True, bottom=True)
    footnote2 = "2020-21 data is being processed and will be added at a later date"
    footnote1 = "2018-19 data is being processed and will be added at a later date"
    ax.annotate(
        f"{footnote1}\n{footnote2}",
        xy=(0.76, -0.25),
        xycoords="axes fraction",
        ha="right",
        va="center",
        fontsize=12,
    )
    return fig


# mean_trends(arable_means, grass_means,total_means)

# COMMAND ----------

# wfm_both =  wfm.loc[wfm.arable & wfm.improved_grassland,:]
# wfm_both = wfm_both.pivot(columns='year', values='bare_soil_percent')
# both_means = pd.DataFrame(wfm_both.mean(),columns=['bare_soil_both'])
# print(wfm_both.shape)
# both_means


# COMMAND ----------

df1 = arable_means.copy()
df2 = grass_means.copy()
df3 = total_means.copy()


sns.set_palette("husl")
sns.set_style("whitegrid")
sns.set_context(font_scale=1.3)


fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(15, 8), squeeze=False, sharey=True)
# plot 3 dfs
for j, df in enumerate([df3, df1, df2]):
    axs[0, j].plot(
        df.year,
        df[df.columns[0]],
        marker="o",
        mec="white",
        linewidth=0.6,
        color="#00A33B",
        markersize=10,
    )
    axs[0, j].grid(visible=False, which="both", axis="x")
    axs[0, j].set_xlabel("Winter")
    axs[0, j].xaxis.set_major_locator(mdates.YearLocator())
    axs[0, j].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: format_winter(mdates.num2date(x))))


axs[0, 0].set_ylim(0, 0.37)
axs[0, 0].set_ylabel("Bare soil percentage")
axs[0, 0].yaxis.set_major_formatter(mtick.PercentFormatter(1, decimals=0))

axs[0, 0].set_title("All fields")
axs[0, 1].set_title("Arable & horticultural fields")
axs[0, 2].set_title("Improved grassland")

# adding annotations to arable and improved grassland plots
vpos = [0.35, 0.23]
xy_coords = [(0.05, 0.97), (0.05, 0.65)]
for j, (y, xy) in enumerate(zip(vpos, xy_coords)):
    axs[0, j + 1].axhline(y, linewidth=1.0, color="#00A33B", linestyle="--")
    axs[0, j + 1].annotate(
        "Baseline used in payment calculation",
        color="#00A33B",
        xy=xy,
        xycoords="axes fraction",
        ha="left",
        va="center",
        fontsize=10,
    )

title = "Mean bare soil over winter in fields across England"
fig.suptitle(title, x=0.075, ha="left", fontsize="x-large")


# COMMAND ----------

df1 = arable_means.copy()
df2 = grass_means.copy()
df3 = total_means.copy()


def yearly_average_wrt_baselines(df1, df2, df3, arable_dict, grass_dict):
    """
    Creating a figure for the average yearly bare soil percentage with resect to the baselines.
    Parameters:
    - df1: arable mean dataframe
    - df2: grassland mean dataframe
    - df3: total means dataframe
    - arable_dict: information in the form of {"Arable & hrticultural": baseline number}
    - grasslande_dict: information in the form of {"Improved grassland": baseline number}
    Returns:
    - Figure
    """
    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context(font_scale=1.3)

    fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(14, 5), squeeze=False, sharey=True)
    axs[0, 0].plot(
        df3.year,
        df3[df3.columns[0]],
        marker="o",
        mec="white",
        linewidth=0.6,
        color="#00A33B",
        markersize=10,
    )
    axs[0, 2].plot(
        df2.year,
        df2[df2.columns[0]],
        marker="o",
        linewidth=0.6,
        mec="white",
        color="#00A33B",
        markersize=10,
    )
    axs[0, 1].plot(
        df1.year,
        df1[df1.columns[0]],
        marker="o",
        linewidth=0.6,
        mec="white",
        color="#00A33B",
        markersize=10,
    )
    for c in range(3):
        if c == 0:
            axs[0, 0].set_title("All fields")
            axs[0, c].set_ylim(0, 0.37)
            axs[0, c].set_ylabel("Bare soil percentage")
            axs[0, c].yaxis.set_major_formatter(mtick.PercentFormatter(1, decimals=0))


axs[0, 0].grid(visible=False, which="both", axis="x")
axs[0, 0].set_xlabel("Winter")
axs[0, 0].xaxis.set_major_locator(mdates.YearLocator())
axs[0, 0].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: format_winter(mdates.num2date(x))))

axs[0, 1].grid(visible=False, which="both", axis="x")
axs[0, 1].set_title("Arable & horticultural fields")
axs[0, 1].axhline(0.35, linewidth=1.0, color="#00A33B", linestyle="--")
axs[0, 1].annotate(
    "Baseline used in payment calculation",
    color="#00A33B",
    xy=(0.05, 0.97),
    xycoords="axes fraction",
    ha="left",
    va="center",
    fontsize=10,
)
axs[0, 1].set_xlabel(
    "Winter",
)
axs[0, 1].xaxis.set_major_locator(mdates.YearLocator())
axs[0, 1].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: format_winter(mdates.num2date(x))))

axs[0, 2].grid(visible=False, which="both", axis="x")
axs[0, 2].axhline(0.23, linewidth=1.0, color="#00A33B", linestyle="--")
axs[0, 2].annotate(
    "Baseline used in payment calculation",
    color="#00A33B",
    xy=(0.05, 0.65),
    xycoords="axes fraction",
    ha="left",
    va="center",
    fontsize=10,
)
axs[0, 2].set_title("Improved grassland")
axs[0, 2].set_xlabel("Winter")
axs[0, 2].xaxis.set_major_locator(mdates.YearLocator())
axs[0, 2].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: format_winter(mdates.num2date(x))))

title = "Mean bare soil over winter in fields across England"
fig.suptitle(title, x=0.075, ha="left", fontsize="x-large")

sns.despine(top=True, right=True, left=True, bottom=True)
# footnote = "2018-19 and 2020-21 data is being processed and will be added at a later date."
# fig.supxlabel(f'\n{footnote}', x=0.075, y=-0.1, ha="left", fontsize="small", wrap=True)
fig.show()

# COMMAND ----------

arable_df = wfm.loc[wfm.arable, ["bare_soil_percent", "year"]]
arable_df = arable_df.pivot(columns="year", values="bare_soil_percent")
arable_df.loc[:, "baseline_flag"] = np.where(arable_df[2023] > 0.35, 1, 0)
print(f"There are {(arable_df.baseline_flag).mean():.2%} of parcels that dont hit the baseline.")

# COMMAND ----------

grass_df = wfm.loc[wfm.improved_grassland, ["bare_soil_percent", "year"]]
grass_df = grass_df.pivot(columns="year", values="bare_soil_percent")
grass_df.loc[:, "baseline_flag"] = np.where(grass_df[2023] > 0.23, 1, 0)
print(f"There are {(grass_df.baseline_flag).mean():.2%} of parcels that dont hit the baseline.")
