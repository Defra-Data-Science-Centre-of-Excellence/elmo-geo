# Databricks notebook source
# MAGIC %md # Bare Soil Survey Analysis
# MAGIC We have received survey data so we can evaluate model analysis.
# MAGIC We have cleaned the data in the bare_soil_survey_data_cleaning nottebook.
# MAGIC
# MAGIC In this notwbook, we will:
# MAGIC - Make a regression model for the data
# MAGIC - Use statistical methods to analyse the model
# MAGIC - Understand what the model is saying

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import numpy as np
import pandas as pd
from patsy import dmatrices  # to create design matrices
import seaborn as sns
import statsmodels.api as sm


from functools import partial
from scipy.stats import kstest, shapiro, norm

# COMMAND ----------

year = 2023
source_data = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data/survey_NDVI_dataset.csv"

# COMMAND ----------

pdf = pd.read_parquet(source_data)
print(pdf.head(2))
pdf.describe()

# COMMAND ----------

# linear regression
sns.set_theme(style="darkgrid")
sns.set_context("talk")

g = sns.jointplot(
    x="NDVI",
    y="bareground_percent_survey",
    data=pdf,
    kind="reg",
    truncate=False,
    height=8,
    xlim=(0, 1),
    ylim=(0, 1),
    color="#00A33b",
    palette="husl",
    space=0.05,
)
g.ax_joint.xaxis.set_major_formatter(PercentFormatter(xmax=1))
g.ax_joint.yaxis.set_major_formatter(PercentFormatter(xmax=1))
g.ax_joint.set_xlabel("NDVI, from Sentinel 2A")
g.ax_joint.set_ylabel("Assessed bare ground")

# COMMAND ----------

y, X = dmatrices("bareground_percent_survey ~NDVI", data=pdf, return_type="dataframe")

mod = sm.GLS(y, X)  # Describe model - ordinart least squares
res = mod.fit()  # Fit model - fits model onto data
print(res.summary())

# COMMAND ----------

sns.residplot(x="NDVI", y="bareground_percent_survey", data=pdf)

# COMMAND ----------

a = sm.qqplot(pdf["NDVI"], line="s")
a.suptitle("NDVI QQ plot")

# COMMAND ----------

b = sm.qqplot(pdf["bareground_percent_survey"], line="s")
b.suptitle("Survey QQ plot")

# COMMAND ----------

c = sm.qqplot(res.resid, fit=True, line="45")
c.suptitle("Model QQ plot")

# COMMAND ----------

# rainbow test - check if linear model is best
sm.stats.linear_rainbow(res)

# COMMAND ----------

if res.params[1] > 0:
    comment_corr = "This means that it is a positive carroelation."
elif res.params[1] < 0:
    comment_corr = "This means that it is a negative carroelation."

if res.pvalues[1] <= 0.05:
    comment_coef_1 = "This means we can reject the hypothesis that states"
elif res.pvalues[1] > 0.05:
    comment_coef_1 = "This means we can accept the hypothesis that states"

print(
    f"The model parameters are {res.params[0]} + {res.params[1]} NDVI"
    + f"{comment_corr} The p alues for NDVI and intercept are {res.pvalues[1]}"
    f" and {res.pvalues[0]} respectively. "
)

# COMMAND ----------

# MAGIC %md
# MAGIC The above model we are creating is:
# MAGIC
# MAGIC bare soil % = 0.9 - 0.92 NDVI
# MAGIC
# MAGIC The p values for both the NDVI coefficient and the intercept
# MAGIC are both negligible. This means the coefficients are significant
# MAGIC and that the model with non zero coefficients describes the data
# MAGIC better than a model with coefficients that are zero.
# MAGIC
# MAGIC Rainbow test for linearity states the null hypothesis to be
# MAGIC that the relationship is properly modelled as linear. Our p value
# MAGIC of 0.005, meaning we may have sufficient evidence to reject the null
# MAGIC hypothesis.
# MAGIC
# MAGIC ##### Residule Analysis
# MAGIC We have plotted the residules of the model with the data. This
# MAGIC shows that each variable is linear and so is the model residukles
# MAGIC (the differences between actual data and modelled data outputs).
# MAGIC
# MAGIC ### Correlation Analysis
# MAGIC As we have seen in the above figure, we see that the variables NDVI and
# MAGIC bare soil percent survey are not entirely normally distributed. This
# MAGIC means we will use Kendall coefficient. Pearson's Correlation coefficient
# MAGIC is used only when two linearly related, normally distributed variables
# MAGIC  are to be used to find their relationship.
# MAGIC
# MAGIC Kendall is a robust non-parametric correlation test that will
# MAGIC provide a statistic for the relationship. The variables
# MAGIC required need to be (ordinal or continuous) and (is desirable
# MAGIC to be) monotonic. From the figure above, we see the data is
# MAGIC mostly monotonic and the data is continuous between a range of
# MAGIC [0,1]. This means the assumptions are satisfied and we may use
# MAGIC this test.
# MAGIC
# MAGIC Below, we see that the Kendall coefficient is -0.48. This means that
# MAGIC there is a weak negative correlation between the two variables.

# COMMAND ----------

pdf[["NDVI", "bareground_percent_survey"]].corr(method="kendall")

# COMMAND ----------

pdf[["NDVI", "bareground_percent_survey"]].corr(method="spearman")

# COMMAND ----------


def test_normality(data: pd.Series):
    tests = {
        "Kolmogrorov-Smirnov": partial(kstest, cdf="norm"),
        "Shapiro-Wilk": shapiro,
    }
    for name, test in tests.items():
        res = test(pdf.NDVI)
        print(
            f"{name} test statistic is ", f"{res.statistic:.4f} with a p-value of {res.pvalue:.4f}"
        )
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


fig, _ = test_normality(pdf.NDVI)
fig.show()

fig, _ = test_normality(pdf.bareground_percent_survey)
fig.show()
