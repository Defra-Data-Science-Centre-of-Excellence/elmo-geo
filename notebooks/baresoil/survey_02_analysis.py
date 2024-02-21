# MAGIC %md # Bare Soil Survey Analysis
# MAGIC We have received survey data so we can evaluate model analysis.
# MAGIC We have cleaned the data in the bare_soil_survey_data_cleaning nottebook.
# MAGIC
# MAGIC In this notwbook, we will:
# MAGIC - Make a regression model for the data
# MAGIC - Use statistical methods to analyse the model

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2
# %pip install -U statsmodels

# COMMAND ----------

from functools import partial

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
from matplotlib.ticker import PercentFormatter
from patsy import dmatrices  # to create design matrices
from scipy.stats import kstest, norm, shapiro

# COMMAND ----------

dataset = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data/survey_ndvi_dataset.csv"
pdf = pd.read_parquet(dataset)
pdf.head(2)

# COMMAND ----------

pdf.describe()

# COMMAND ----------

pdf["Field type"] = pdf["field_type"]

# linear regression
sns.set_theme(style="whitegrid")
sns.set_context("talk")
sns.despine(top=True, bottom=True, left=True, right=True)
x_col = "ndvi"
y_col = "bareground_percent_survey"
hue_col = "Field type"
colour_palette = ["#00A33b", "#999999"]
g = sns.jointplot(
    data=pdf,
    x=x_col,
    y=y_col,
    hue=hue_col,
    height=9,
    palette=colour_palette,
)

for _, gr in pdf.groupby(hue_col):
    if len(gr) > pdf.shape[0] / 2:
        sns.regplot(
            x=x_col,
            y=y_col,
            data=gr,
            scatter=False,
            ax=g.ax_joint,
            truncate=False,
            color=colour_palette[0],
        )
    else:
        sns.regplot(
            x=x_col,
            y=y_col,
            data=gr,
            scatter=False,
            ax=g.ax_joint,
            truncate=False,
            color=colour_palette[1],
        )

g.plot_marginals(
    sns.kdeplot,
    data=pdf,
    hue="Field type",
    palette=["#00A33b", "#999999"],
)

g.ax_joint.set_xlim(0, 1)
g.ax_joint.set_ylim(0, 1)
g.ax_joint.xaxis.set_major_formatter(PercentFormatter(xmax=1))
g.ax_joint.yaxis.set_major_formatter(PercentFormatter(xmax=1))
g.ax_joint.set_xlabel("NDVI, from Sentinel 2A")
g.ax_joint.set_ylabel("Assessed bare ground")
g.ax_marg_x.set_title("R2=0.7")


# COMMAND ----------

# multiple linear regression with interaction between ndvi and field_type
y, X = dmatrices("bareground_percent_survey~ndvi*field_type", data=pdf, return_type="dataframe")

mod = sm.GLS(y, X)  # Describe model - ordinart least squares
res = mod.fit()  # Fit model - fits model onto data
print(res.summary())

# COMMAND ----------

# model residule plot - to check if model is good
sns.residplot(x="ndvi", y="bareground_percent_survey", data=pdf)

# COMMAND ----------

# qq plot to check visually for normality
a = sm.qqplot(pdf["ndvi"], line="s")
a.suptitle("NDVI QQ plot")

# COMMAND ----------

# qq plot to check visually for normality
b = sm.qqplot(pdf["bareground_percent_survey"], line="s")
b.suptitle("Survey QQ plot")

# COMMAND ----------

# qq plot to check visually for normality
c = sm.qqplot(res.resid, fit=True, line="45")
c.suptitle("Model QQ plot")

# COMMAND ----------

if res.params[1] > 0:
    comment_corr = "This means that it has a positive carroelation."
elif res.params[1] < 0:
    comment_corr = "This means that it has a negative carroelation."

if res.pvalues[1] <= 0.05:
    comment_outcome = "This means we can reject the hypothesis which states the " "coefficients in the model do not describe the data."
elif res.pvalues[1] > 0.05:
    comment_outcome = (
        "This means we can don't have sufficient evidence " "to reject the hypothesis ehich states the " "coefficients in the model do not describe the data."
    )

print(
    f"All p values for the multiple regression model are significant. Each coefficients p values "
    f"are {res.pvalues[0]:.2%}, {res.pvalues[1]:.2%}, {res.pvalues[2]:.2%}, {res.pvalues[3]:.2%} "
    f"respectively which means they are all significant to describe the model."
)

# COMMAND ----------

kendall_corr = pdf[["ndvi", "bareground_percent_survey"]].corr(method="kendall")
spearmans_corr = pdf[["ndvi", "bareground_percent_survey"]].corr(method="spearman")

if kendall_corr.iloc[0, 1] < 0:
    comment_k = "This means that the two variables have a negative correlation."
else:
    comment_k = "This means that the two variables have a positive correlation."
if spearmans_corr.iloc[0, 1] < 0:
    comment_s = "This means that the two variables have a negative correlation."
else:
    comment_s = "This means that the two variables have a positive correlation."

print(
    f"We now will check the correlation coefficent for these variables. We find"
    f"that the kendals coor is {kendall_corr.iloc[0,1]:.3f} {comment_k} We also "
    f"conducted a spearmans correlation coeffienct which is "
    f"{spearmans_corr.iloc[0,1]:.3f}. {comment_s}"
)

# COMMAND ----------

# normality check using numbers instead of visualising
pdf = pdf.dropna()


def test_normality(data: pd.Series):
    tests = {
        "Kolmogrorov-Smirnov": partial(kstest, cdf="norm"),
        "Shapiro-Wilk": shapiro,
    }
    for name, test in tests.items():
        res = test(pdf.ndvi)
        print(f"{name} test statistic is ", f"{res.statistic:.4f} with a p-value of {res.pvalue:.4f}")
        if res.pvalue > 0.05:
            print(
                "As the p-value is > 0.05, we accept the null hypothesis ",
                "that the data is normally distributed.",
            )
        else:
            print(
                "As the p-value is <= 0.05, we reject the null hypothesis",
                " that the data is normally distributed.",
            )
    fig, ax = plt.subplots(figsize=(6, 3))
    mu, std = norm.fit(data)
    data.hist(bins="auto", ax=ax, density=True, alpha=0.5)
    xmin, xmax = ax.get_xlim()
    x = np.linspace(xmin, xmax, 100)
    p = norm.pdf(x, mu, std)
    ax.plot(x, p, "k", linewidth=2)
    ax.set_title(data.name)
    return fig, ax


fig, _ = test_normality(pdf.ndvi.dropna())
fig.show()

fig, _ = test_normality(pdf.bareground_percent_survey)
fig.show()
