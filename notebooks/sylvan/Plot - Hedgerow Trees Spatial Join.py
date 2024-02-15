# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Visualise spatial join

# COMMAND ----------

treesSubDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
hrSubDF = hrDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")
hrtreesSubDF = treeFeaturesDF.filter(f"id_parcel like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

hrtreesSubDF.show(5)

# COMMAND ----------

# Define a bounding box to use for the map
xmin = 462000
xmax = 463000
ymin = 255000
ymax = 256000

bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin)]

bbox = Polygon(bbox_coords)
gdfbb = gpd.GeoDataFrame({"geometry": [bbox]}, crs="EPSG:27700").to_crs("EPSG:3857")

# COMMAND ----------

# Convert to geopandas and plot

hrSubDF = hrSubDF.withColumn("wkb", F.col("geometry")).withColumn(
    "geometry", F.expr(f"ST_Buffer(ST_GeomFromWKB(wkb), {hedgerows_buffer_distance})")
)
treesSubDF = treesSubDF.withColumn("geometry", F.expr("ST_Point(top_x, top_y)"))

hrSubDF = hrSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geometry
                                )
                                """
    )
)

treesSubDF = treesSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geometry
                                )
                                """
    )
)

hrtreesSubDF = hrtreesSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geom_right
                                )
                                """
    )
)

gdf_tile_trees = gpd.GeoDataFrame(treesSubDF.toPandas(), geometry="geometry")

ghrtreesDF = gpd.GeoDataFrame(hrtreesSubDF.toPandas(), geometry="geom_right")

ghrDFr = gpd.GeoDataFrame(hrSubDF.toPandas(), geometry="geometry")

# COMMAND ----------

ghrDFr_plot = ghrDFr.loc[ghrDFr["geometry"].map(lambda g: g.intersects(bbox))]

# COMMAND ----------

gdf_tile_trees.head()

# COMMAND ----------

f, ax = plt.subplots(figsize=(15, 15))

cmap = plt.get_cmap("gist_rainbow")
parcels = ghrDFr_plot["PARCEL_FK"].unique()
cmap_inputs = np.linspace(0, 1, len(parcels))
# parcel_colours = cmap(cmap_inputs)
dict_parcel_colours = dict(zip(parcels, cmap_inputs))
ghrDFr_plot["c"] = ghrDFr_plot["PARCEL_FK"].replace(dict_parcel_colours)

ghrDFr_plot.plot(ax=ax, facecolor="c", cmap=mpl.cm.gist_rainbow, alpha=0.5)


gdf_tile_trees.plot(ax=ax, markersize=45, alpha=1, marker="x", color="black")

ghrtreesDF.plot(ax=ax, markersize=50, alpha=1, marker="o", color="forestgreen")

ax.set_xlim(xmin=462000, xmax=463000)
ax.set_ylim(ymin=255000, ymax=256000)
ax.set_axis_off()
ctx.add_basemap(ax=ax, source=ctx.providers.OpenStreetMap.Mapnik, crs="EPSG:27700")

# COMMAND ----------

f, ax = plt.subplots(figsize=(15, 15))
ghrDFr_plot.plot(ax=ax, facecolor="cornflowerblue", alpha=0.5)
gdf_tile_trees.plot(ax=ax, markersize=45, alpha=1, marker="x", color="black")
ghrtreesDF.plot(ax=ax, markersize=50, alpha=1, marker="o", color="forestgreen")

ax.set_xlim(xmin=462000, xmax=463000)
ax.set_ylim(ymin=255000, ymax=256000)
ax.set_axis_off()

# COMMAND ----------

# Investigating hedges - are there duplicated geometries?
ghrDFr.FEATURE_ID.duplicated().value_counts()

# COMMAND ----------

ghrDFr["SHEET_PARCEL_ID"] = ghrDFr.apply(
    lambda row: f"{row['REF_PARCEL_SHEET_ID']}{row['REF_PARCEL_PARCEL_ID']}"
)
ghrDFr.SHEET_PARCEL_ID.duplicated().value_counts()
