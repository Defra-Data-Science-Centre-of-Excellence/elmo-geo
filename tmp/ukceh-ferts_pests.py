# Databricks notebook source
# MAGIC %md
# MAGIC # Fertiliser and Pesticides Maps joined to Parcels
# MAGIC rs.sjoin
# MAGIC l/ha
# MAGIC
# MAGIC https://bdcertification.org.uk/wp/wp-content/uploads/2022/04/BDA-Certification-Organic-Production-Standards-2022-.pdf
# MAGIC
# MAGIC
# MAGIC Check wfm organic fert/pest level
# MAGIC

# COMMAND ----------

import os
from io import StringIO

import pandas as pd
import rioxarray as rxr
import xarray as xr
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rioxarray.exceptions import NoDataInBounds, OneDimensionalRaster

import elmo_geo
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_ODS, FOLDER_STG

elmo_geo.register()

# COMMAND ----------

sf_parcels = dbfs(FOLDER_ODS, True) + "/rpa-parcels-2021_03_16.parquet"
f_ferts = FOLDER_STG + "/ukceh-fertilisers-2010_2015/"
f_pests = FOLDER_STG + "/ukceh-pesticides-2012_2017/"
f_wfm_farm = FOLDER_STG + "/wfm-farm-2023_06_09.feather"
f_wfm_field = FOLDER_STG + "/wfm-field-2023_06_09.feather"

f_fert = FOLDER_ODS + "/elmo_geo-fertilisers-2023_10_05.parquet"
f_pest = FOLDER_ODS + "/elmo_geo-pesticides-2023_10_05.parquet"

f_out = FOLDER_ODS + "/awest-wfm_fert_pest-2023_10_17.feather"
f_sum = FOLDER_ODS + "/awest-wfm_fert_pest_sum-2023_10_26.feather"

# COMMAND ----------

# BboxType = Union[list, tuple][[Union(float, int)]*4]  # xmin, ymin, xmax, ymax


def bbox_overlap(bbox0, bbox1):
    x0, y0, x1, y1 = bbox0
    x2, y2, x3, y3 = bbox1
    return x0 <= x2 and y0 <= y2 and x3 <= x1 and y3 <= y1


def equal_res(da0, da1):
    return all(da0.x == da1.x) and all(da0.y == da1.y)


def da_clip_mean(col, da):
    @F.udf(T.ArrayType(T.DoubleType()))
    def _udf(geometry):
        try:
            return da.rio.clip([geometry], all_touched=True).mean(dim=("x", "y")).data.tolist()
        except (OneDimensionalRaster, NoDataInBounds):
            return

    return _udf(col)


def da_clip_sum(col, da):
    @F.udf(T.ArrayType(T.DoubleType()))
    def _udf(geometry):
        try:
            return da.rio.clip([geometry], all_touched=True).sum(dim=("x", "y")).data.tolist()
        except (OneDimensionalRaster, NoDataInBounds):
            return

    return _udf(col)


# TODO: pudf_da_clip_mean


def fert_namer(f):
    return "fertiliser_{0}_prediction\nfertiliser_{0}_uncertainty".format(f.split("_")[1]).split()


def pest_namer(f):
    return "pesticide_{0}_prediction\npesticide_{0}_uncertainty".format(f.split(".")[0]).split()


# COMMAND ----------

# Parcels
sdf_parcels = spark.read.format("geoparquet").load(sf_parcels).repartition(2000)

display(sdf_parcels)

# COMMAND ----------

# Fertilisers
da_fert = xr.concat(
    [rxr.open_rasterio(f_ferts + f, masked=True).assign_coords(band=fert_namer(f)) for f in os.listdir(f_ferts)],
    dim="band",
).fillna(0)


sdf_fert = sdf_parcels.withColumn("_out", da_clip_mean("geometry", da_fert)).select(
    "id_parcel",
    *[F.expr(f"_out[{i}] AS {name}") for i, name in enumerate(da_fert.band.data)],
)

sdf_fert.write.parquet(dbfs(f_fert, True), mode="overwrite")
display(sdf_fert)

# COMMAND ----------

# Pesticides
da_pest = xr.concat(
    [rxr.open_rasterio(f_pests + f, masked=True).assign_coords(band=pest_namer(f)) for f in os.listdir(f_pests)],
    dim="band",
).fillna(0)


sdf_pest = sdf_parcels.withColumn("_out", da_clip_mean("geometry", da_pest)).select(
    "id_parcel",
    *[F.expr(f"_out[{i}] AS {name}") for i, name in enumerate(da_pest.band.data)],
)

dbutils.fs.rm(dbfs(f_pest, True), recurse=True)
sdf_pest.write.parquet(dbfs(f_pest, True))  # , mode='overwrite')
display(sdf_pest)

# COMMAND ----------

# Joined
df_wfm_farm = pd.read_feather(f_wfm_farm)
df_wfm_field = pd.read_feather(f_wfm_field)
df_fert = pd.read_parquet(f_fert)
df_pest = pd.read_parquet(f_pest)


df_wfm = pd.DataFrame.merge(
    df_wfm_farm[["id_business", "farm_type"]],
    df_wfm_field[
        [
            "id_business",
            "id_parcel",
            "participation_schemes",
            "ha_parcel_geo",
            "ha_parcel_uaa",
        ]
    ],
    on="id_business",
    how="outer",
)

df = df_wfm.merge(df_fert, on="id_parcel", how="outer").merge(df_pest, on="id_parcel", how="outer")


df.to_feather(f_out)
df

# COMMAND ----------

csv = """Name,chemgroup
1-naphthylacetic acid,Growth regulator
2-chloroethylphosphonic acid,Growth regulator
"2,4-D",Herbicide
"2,4-DB",Herbicide
6-benzyladenine,Growth regulator
6-benzylaminopurine,Growth regulator
Abamectin,Acaricide
Acetamiprid,Insecticide
Aclonifen,Herbicide
Alpha-cypermethrin,Insecticide
Aluminium ammonium sulphate,Repellent
Ametoctradin,Fungicide
Amidosulfuron,Herbicide
Aminopyralid,Herbicide
Amisulbrom,Fungicide
Amitrole,Herbicide
Asulam,Herbicide
Azoxystrobin,Fungicide
Bentazone,Herbicide
Benthiavalicarb-isopropyl,Fungicide
Benthiavalicarb,Fungicide
Benzovindiflupyr,Fungicide
Beta-cyfluthrin,Insecticide
Bifenazate,Acaricide
Bifenox,Herbicide
Bifenthrin,Insecticide
Bitertanol,Fungicide
Bixafen,Fungicide
Boscalid,Fungicide
Bromoxynil,Herbicide
Bromuconazole,Fungicide
Bupirimate,Fungicide
Calcium chloride,Repellent
Captan,Fungicide
Carbendazim,Fungicide
Carbetamide,Herbicide
Carbosulfan,Insecticide
Carboxin,Fungicide
Carfentrazone-ethyl,Herbicide
Chitosan hydrochloride,Fungicide
Chlorantraniliprole,Insecticide
Chloridazon,Herbicide
Chlormequat chloride,Growth regulator
Chlormequat,Growth regulator
Chloropicrin,Soil sterilant
Chlorothalonil,Fungicide
Chlorotoluron,Herbicide
Chlorpropham,Herbicide
Chlorpyrifos,Insecticide
Clethodim,Herbicide
Clodinafop-propargyl,Herbicide
Clofentezine,Acaricide
Clomazone,Herbicide
Clopyralid,Herbicide
Cloquintocet-mexyl,Herbicide
Clothianidin,Insecticide
Copper oxychloride,Fungicide
Copper sulphate,Fungicide
Cyantraniliprole,Insecticide
Cyazofamid,Fungicide
Cycloxydim,Herbicide
Cyflufenamid,Fungicide
Cymoxanil,Fungicide
Cypermethrin,Insecticide
Cyproconazole,Fungicide
Cyprodinil,Fungicide
Dazomet,Soil sterilant
Deltamethrin,Insecticide
Desmedipham,Herbicide
Dicamba,Herbicide
Dichlorprop-P,Herbicide
Diclofop-methyl,Herbicide
Difenoconazole,Fungicide
Diflubenzuron,Insecticide
Diflufenican,Herbicide
Dimethachlor,Herbicide
Dimethenamid-P,Herbicide
Dimethoate,Insecticide
Dimethomorph,Fungicide
Dimoxystrobin,Fungicide
Diquat,Herbicide
Dithianon,Fungicide
Dodine,Fungicide
Epoxiconazole,Fungicide
Esfenvalerate,Insecticide
Ethametsulfuron-methyl,Herbicide
Ethofumesate,Herbicide
Ethoprophos,Insecticide;Nematicide
Etoxazole,Acaricide
Famoxadone,Fungicide
Fatty acids C7-C20,Insecticide
Fatty acids,Insecticide
Fenamidone,Fungicide
Fenbuconazole,Fungicide
Fenhexamid,Fungicide
Fenoxaprop-P-ethyl,Herbicide
Fenoxycarb,Insecticide
Fenpropidin,Fungicide
Fenpropimorph,Fungicide
Fenpyrazamine,Fungicide
Fenpyroximate,Acaricide
Ferric phosphate,Molluscicide
Fipronil,Insecticide
Flazasulfuron,Herbicide
Flonicamid,Insecticide
Florasulam,Herbicide
Fluazifop-P-butyl,Herbicide
Fluazinam,Fungicide
Fludioxonil,Fungicide
Flufenacet,Herbicide
Flumioxazine,Herbicide
Fluopicolide,Fungicide
Fluopyram,Fungicide
Fluoxastrobin,Fungicide
Flupyrsulfuron-methyl,Herbicide
Fluquinconazole,Fungicide
Fluroxypyr,Herbicide
Flurtamone,Herbicide
Flusilazole,Fungicide
Flutolanil,Fungicide
Flutriafol,Fungicide
Fluxapyroxad,Fungicide
Folpet,Fungicide
Foramsulfuron,Herbicide
Fosetyl-aluminium,Fungicide
Fosthiazate,Insecticide;Nematicide
Fuberidazole,Fungicide
Garlic,Physical control
Gibberellic acid,Growth regulator
Gibberellins,Growth regulator
Glufosinate-ammonium,Herbicide
Glyphosate,Herbicide; Desiccant
Guazatine,Fungicide
Halauxifen-methyl,Herbicide
Hymexazol,Fungicide
Imazalil,Fungicide
Imazamox,Herbicide
Imazaquin,Growth regulator
Imazosulfuron,Herbicide
Imidacloprid,Insecticide
Indoxacarb,Insecticide
Iodosulfuron-methyl-sodium,Herbicide
Ioxynil,Herbicide
Ipconazole,Fungicide
Iprodione,Fungicide
Isoproturon,Herbicide
Isopyrazam,Fungicide
Isoxaben,Herbicide
Isoxaflutole,Herbicide
Kaolin,Repellent
Kresoxim-methyl,Fungicide
Lambda-cyhalothrin,Insecticide
Lenacil,Herbicide
Linuron,Herbicide
Maleic hydrazide,Growth regulator
Mancozeb,Fungicide
Mandipropamid,Fungicide
MCPA,Herbicide
MCPB,Herbicide
Mecoprop-P,Herbicide
Mefentrifluconazole,Fungicide
Mepanipyrim,Fungicide
Mepiquat chloride,Growth regulator
Mepiquat,Growth regulator
Meptyldinocap,Fungicide
Mesosulfuron-methyl,Herbicide
Mesotrione,Herbicide
Metalaxyl-M,Fungicide
Metalaxyl,Fungicide
Metaldehyde,Molluscicide
Metam-sodium,Soil sterilant
Metamitron,Herbicide; Growth regulator
Metazachlor,Herbicide
Metconazole,Fungicide; Growth regulator
Methiocarb,Molluscicide
Methoxyfenozide,Insecticide
Metobromuron,Herbicide
Metrafenone,Fungicide
Metribuzin,Herbicide
Metsulfuron-methyl,Herbicide
Myclobutanil,Fungicide
Napropamide,Herbicide
Nicosulfuron,Herbicide
Oxamyl,Insecticide
Oxathiapiprolin,Fungicide
Paclobutrazol,Growth regulator
Paraquat,Herbicide
Penconazole,Fungicide
Pencycuron,Fungicide
Pendimethalin,Herbicide
Penflufen,Fungicide
Penthiopyrad,Fungicide
Permethrin,Insecticide
Peroxyacetic acid,Disinfectant
Phenmedipham,Herbicide
Picloram,Herbicide
Picolinafen,Herbicide
Picoxystrobin,Fungicide
Pinoxaden,Herbicide
Pirimicarb,Insecticide
Potassium hydrogen carbonate,Fungicide
Potassium phosphonate (phosphite),Fungicide
Prochloraz,Fungicide
Prohexadione-calcium,Growth regulator
Prohexadione,Growth regulator
Propachlor,Herbicide
Propamocarb hydrochloride,Fungicide
Propaquizafop,Herbicide
Propiconazole,Fungicide
Propoxycarbazone-sodium,Herbicide
Propyzamide,Herbicide
Proquinazid,Fungicide
Prosulfocarb,Herbicide
Prosulfuron,Herbicide
Prothioconazole,Fungicide
Pymetrozine,Insecticide
Pyraclostrobin,Fungicide
Pyraflufen-ethyl,Desiccant
Pyrethrins,Insecticide
Pyridate,Herbicide
Pyrimethanil,Fungicide
Pyriofenone,Fungicide
Pyroxsulam,Herbicide
Quinmerac,Herbicide
Quinoxyfen,Fungicide
Quizalofop-P-ethyl,Herbicide
Quizalofop-P-tefuryl,Herbicide
Rimsulfuron,Herbicide
S-metolachlor,Herbicide
Sedaxane,Fungicide
Silthiofam,Fungicide
Simazine,Herbicide
Sodium chloride,Herbicide
Spinosad,Insecticide
Spirodiclofen,Acaricide
Spiromesifen,Insecticide
Spirotetramat,Insecticide
Spiroxamine,Fungicide
Sulfosulfuron,Herbicide
Sulphur,Sulphur
Tau-fluvalinate,Insecticide
Tebuconazole,Fungicide
Tebufenpyrad,Acaricide/insecticide
Tefluthrin,Insecticide
Tepraloxydim,Herbicide
Terbuthylazine,Herbicide
Thiabendazole,Fungicide
Thiacloprid,Insecticide
Thiamethoxam,Insecticide
Thiencarbazone-methyl,Herbicide
Thifensulfuron-methyl,Herbicide
Thiophanate-methyl,Fungicide
Thiram,Fungicide
Tolclofos-methyl,Fungicide
Tralkoxydim,Herbicide
Tri-allate,Herbicide
Triadimenol,Fungicide
Triazoxide,Fungicide
Tribenuron-methyl,Herbicide
Triclopyr,Herbicide
Trifloxystrobin,Fungicide
Trifluralin,Herbicide
Triflusulfuron-methyl,Herbicide
Trinexapac-ethyl,Growth regulator
Triticonazole,Fungicide
Urea,Fungicide
Zeta-cypermethrin,Insecticide
Zoxamide,Fungicide"""

# COMMAND ----------

lookup = pd.DataFrame(
    {
        "pest": [pest.replace(".tif", "") for pest in os.listdir(f_pests)],
    }
).merge(
    pd.read_csv(StringIO(csv)).assign(
        pest=lambda df: df["Name"].str.lower().str.replace("-", "_").str.replace(",", "_"),
        chemgroup=lambda df: df["chemgroup"].str.split(";").str[0].str.lower(),
    ),
    on="pest",
    how="left",
)

lookup.groupby("chemgroup", dropna=False)["chemgroup"].count()

# COMMAND ----------

df = pd.read_feather(f_out)

df

# COMMAND ----------

# Summary
# df = pd.read_feather(f_out)


df_sum = df[["id_business", "id_parcel", "ha_parcel_geo", "ha_parcel_uaa"]].assign(
    farm_type=df["farm_type"].fillna(0).astype(int),
    participation_schemes=df["participation_schemes"].fillna(0).astype(int),
    tpha_fertiliser_n=df["fertiliser_n_prediction"] / 1e5,
    tpha_fertiliser_p=df["fertiliser_p_prediction"] / 1e5,
    tpha_fertiliser_k=df["fertiliser_k_prediction"] / 1e5,
    # tpha_fertilisers = df[[col for col in df.columns if col.startswith('fertiliser_') and col.endswith('_prediction')]].sum(axis=1) / 1e5,
    # tpha_pesticides = df[[col for col in df.columns if col.startswith('pesticide_') and col.endswith('_prediction')]].sum(axis=1) / 1e5,
)

for chems, chemgroup in chemgroups:
    df_sum[f"tpha_{chemgroup}"] = (
        df[[col for col in df.columns if col.startswith("pesticide_") and col.endswith("prediction") and any(chems in col)]].sum(axis=1) / 1e5
    )


df_sum.to_feather(f_sum)
df_sum
