# Databricks notebook source
# MAGIC %md
# MAGIC # Subdivide
# MAGIC Explore subdividing algorithms, useful large geometries.  Single geometries can be so large they cause OOM errors.  By subdividing them, the geometries can be analysed separately on partitions.  Examples are datasets like flood warning areas, where many organisations provide a single geometry to a large dataset, some of these geometries are for areas around whole river basins.  Another example is none generalised country geometries.  
# MAGIC https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/  
# MAGIC https://gist.github.com/JoaoCarabetta/8a5df60ac0012e56219a5b2dffcb20e3  
# MAGIC
# MAGIC |method|timeit|vertices|increase|conclusion|
# MAGIC |---|---|---|---|---|
# MAGIC | | |49,314|
# MAGIC |fishnet, max_length=100m|3 s ± 214 ms|56,346|14.3%|may as well chip+index|
# MAGIC |katana, max_area=1ha, split by box|407 ms ± 9.3 ms|54,978|11.5%|doesn't suit varied sized geometries|
# MAGIC |delaunay, max_vertices=256|30.8 s ± 129 ms|95502|93.7%|why did I think this was a good idea|
# MAGIC |katana, max_vertices=256, split by box|313 ms ± 4 ms|51,046|3.5%|better suited for subdividing|
# MAGIC |katana, max_vertices=256, split by line|463 ms ± 8.26 ms|51,046|3.5%|more simple cutting method|
# MAGIC |katana, max_vertices=256, split by rect|142 ms ± 2.48 ms|51,027|3.5%|faster cutting method|
# MAGIC

# COMMAND ----------

# MAGIC %matplotlib notebook

# COMMAND ----------

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import box, delaunay_triangles, from_wkt
from shapely.geometry import LineString, MultiPolygon, Polygon
from shapely.ops import split, clip_by_rect
from matplotlib.animation import FuncAnimation

# COMMAND ----------

df = gpd.read_file('/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_water_features/format_GPKG_ngd_water_features/SNAPSHOT_2023_12_16_ngd_water_features/wtr_fts_water.gpkg', rows=100)
polygon = from_wkt('POLYGON((2.0117187499999822 44.38657313925715,-19.433593750000018 19.207272119703983,19.414062499999982 6.904449621538131,64.94140624999999 -3.096801256840523,81.46484374999999 37.21269961002643,45.78124999999998 24.106495997107682,53.69140624999998 51.22054369437158,3.7695312499999822 37.07257833232809,2.0117187499999822 44.38657313925715))')
multipolygon = gpd.read_file(gpd.datasets.get_path('nybb'), rows=1).loc[0, 'geometry']

# COMMAND ----------

def displayGEOM(geometry):
    displayHTML('<svg width=400 height=400 viewBox="{}, {}, {}, {}">{}</svg>'.format(*geometry.bounds, geometry.svg()))

def geometry_flatten(geometry):
    if hasattr(geometry, 'geoms'):  # Multi<Type> / GeometryCollection
      for g in geometry.geoms:
        yield from geometry_flatten(g)
    elif hasattr(geometry, 'interiors'):  # Polygon
        yield geometry.exterior
        yield from geometry.interiors
    else:  # Point / LineString
        yield geometry

def geometry_length(geometry):
    return sum(len(g.coords) for g in geometry_flatten(geometry))

def fishnet(geometry, threshold):
    '''Apply a grid and cut geometries up in that grid
    '''
    def _fn():
        xmin, ymin, xmax, ymax = [int(bound//threshold) for bound in geometry.bounds]
        for i in range(xmin, xmax+1):
            for j in range(ymin, ymax+1):
                g = geometry.intersection(box(i*threshold, j*threshold, (i+1)*threshold, (j+1)*threshold))
                if isinstance(g, MultiPolygon):
                    yield from g.geoms
                elif isinstance(g, Polygon):
                    yield g
                # Ignore: Empty, Points, LineStrings
    return MultiPolygon(_fn())

def geometry_split_box(geometry):
    xmin, ymin, xmax, ymax = geometry.bounds
    width, height = xmax-xmin, ymax-ymin
    if width < height:
        a = box(xmin, ymin, xmax, ymin+height/2)  # bottom
        b = box(xmin, ymin+height/2, xmax, ymax)  # top
    else:
        a = box(xmin, ymin, xmin+width/2, ymax)  # left
        b = box(xmin+width/2, ymin, xmax, ymax)  # right
    a, b = geometry.intersection(a), geometry.intersection(b)
    return [*getattr(a, 'geoms', [a]), *getattr(b, 'geoms', [b])]

def geometry_split_line(geometry):
    xmin, ymin, xmax, ymax = geometry.bounds
    width, height = xmax-xmin, ymax-ymin
    if width < height:
        ymid = ymin+height/2
        line = LineString([[xmin, ymid], [xmax, ymid]])
    else:
        xmid = xmin+width/2
        line = LineString([[xmid, ymin], [xmid, ymax]])
    return split(geometry, line).geoms

def geometry_split_rect(geometry):
    xmin, ymin, xmax, ymax = geometry.bounds
    width, height = xmax-xmin, ymax-ymin
    if width < height:
        ymid = ymin+height/2
        a = clip_by_rect(geometry, xmin, ymin, xmax, ymid)
        b = clip_by_rect(geometry, xmin, ymid, xmax, ymax)
    else:
        xmid = xmin+width/2
        a = clip_by_rect(geometry, xmin, ymin, xmid, ymax)
        b = clip_by_rect(geometry, xmid, ymin, xmax, ymax)
    return [*getattr(a, 'geoms', [a]), *getattr(b, 'geoms', [b])]

def original_threshold(geometry):
    return geometry.area < 10_000

def vertices_threshold(geometry):
    return geometry_length(geometry) <= 256

def katana(geometry: MultiPolygon|Polygon, threshold:callable=vertices_threshold, geometry_split:callable=geometry_split_rect, max_depth:int=100):
    '''If a geometry is passes the threshold, cut it in half, repeat recursively
    '''
    def _fn(geometry, max_depth):
        if max_depth <= 0 or threshold(geometry):
            if not geometry.is_empty and isinstance(geometry, Polygon):
                yield geometry
        elif hasattr(geometry, 'geoms'):  # first expand Multi<type>
            yield from geometry.geoms
        else:  # then subdivide
            for g in geometry_split(geometry):
                yield from _fn(g, max_depth-1)
    return MultiPolygon(_fn(geometry, max_depth))  # drop empty, line

def delaunay(geometry, max_vertices=256):
    '''Split a geometry into triangles, group those triangles into areas.
    '''
    return MultiPolygon(
        gpd.GeoSeries(delaunay_triangles(geometry).geoms)
        .pipe(lambda df: df.intersection(geometry))
        .pipe(lambda df: df[(0<df.area) & (df.geom_type=='Polygon')])
        .reset_index(drop=True).reset_index()
        .assign(index = lambda df: df['index'] // (df.shape[0]//(max_vertices-2)))
        .dissolve(by='index')
        .explode(index_parts=False)
        [0].tolist()
    )

# COMMAND ----------

fig, ax = plt.subplots()
def update(frame):
    ax.clear()
    ax.axis('off')
    g0 = df['geometry'][frame:frame+1]
    g1 = gpd.GeoSeries(g0.apply(delaunay_triangles, tolerance=0.001))
    g0.plot(ax=ax, color='r')
    g1.plot(ax=ax, color='C0', edgecolor='k')
ani = FuncAnimation(fig, update, frames=df.shape[0], interval=200)

displayHTML(ani.to_html5_video())

# COMMAND ----------

print(
    df['geometry'].transform(geometry_length).sum(),
    df['geometry'].transform(fishnet, threshold=100).transform(geometry_length).sum(),
)
%timeit df['geometry'].transform(fishnet, threshold=100)
fishnet(polygon, threshold=100)

# COMMAND ----------

fig, ax = plt.subplots()
def update(frame):
    ax.clear()
    ax.axis('off')
    g0 = df['geometry'][frame:frame+1]
    g1 = g0.transform(fishnet, threshold=100)
    g0.plot(ax=ax, color='r')
    g1.plot(ax=ax, color='C0', edgecolor='k')
ani = FuncAnimation(fig, update, frames=df.shape[0], interval=200)

displayHTML(ani.to_html5_video())

# COMMAND ----------

print(
    df['geometry'].transform(katana, threshold=original_threshold, geometry_split=geometry_split_box).transform(geometry_length).sum(),
    df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_box).transform(geometry_length).sum(),
    df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_line).transform(geometry_length).sum(),
    df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_rect).transform(geometry_length).sum(),
)
%timeit df['geometry'].transform(katana, threshold=original_threshold, geometry_split=geometry_split_box)
%timeit df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_box)
%timeit df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_line)
%timeit df['geometry'].transform(katana, threshold=vertices_threshold, geometry_split=geometry_split_rect)
katana(multipolygon)

# COMMAND ----------

fig, ax = plt.subplots()
def update(frame):
    ax.clear()
    ax.axis('off')
    g0 = df['geometry'][frame:frame+1]
    g1 = g0.transform(katana)
    g0.plot(ax=ax, color='r')
    g1.plot(ax=ax, color='C0', edgecolor='k')
ani = FuncAnimation(fig, update, frames=df.shape[0], interval=200)

displayHTML(ani.to_html5_video())

# COMMAND ----------

print(
    df['geometry'].transform(delaunay).transform(geometry_length).sum(),
)
%timeit df['geometry'].transform(delaunay)
delaunay(multipolygon)

# COMMAND ----------

fig, ax = plt.subplots()
def update(frame):
    ax.clear()
    ax.axis('off')
    g0 = df['geometry'][frame:frame+1]
    g1 = g0.transform(delaunay)
    g0.plot(ax=ax, color='r')
    g1.plot(ax=ax, color='C0', edgecolor='k')
ani = FuncAnimation(fig, update, frames=df.shape[0], interval=200)

displayHTML(ani.to_html5_video())
