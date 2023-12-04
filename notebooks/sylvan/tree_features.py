"""
Functions that produce parcel level features related to the presence of trees.

Tree Features:
- Hedgerow Trees: Number and density (per 100m of hedgerow) of hedgerow trees per parcel
- Boundary trees: Number of trees intersecting the parcel boundary.
- Woody linear features: Number of trees within woody linear features that intersect the parcel boundary.*
- Interior trees: Number of trees in the interior of a parcel.

* Unsure of how to define this feature.
"""
import itertools
from typing import Dict, List, Optional, Tuple

from pyspark.sql import functions as F

from elmo_geo import LOG
from elmo_geo.utils.types import SparkDataFrame, SparkSession

#
# Define functions that are used in geocomputation operations
#

# Define buffer function
buf = lambda col, x: F.expr(f"ST_MakeValid(ST_Buffer({col}, {x}))")

tree_count = lambda x: F.expr(
    f"""
                            CAST(
                                ST_Intersects(
                                    ST_MakeValid(ST_Buffer(geom_left, {x})),
                                    ST_MakeValid(geom_right)
                                    ) AS INTEGER
                                )as c{x}"""
)

tree_intersection_length = lambda x: F.expr(
    f"""
                                            ST_Length(
                                                ST_Intersection(
                                                    ST_MakeValid(ST_Buffer(geom_left, {x})),
                                                    ST_MakeValid(geom_right)
                                                )
                                            ) as c{x}"""
)


def sjoin(
    spark,
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
) -> SparkDataFrame:
    # Rename
    for col in sdf_left.columns:
        if col in sdf_right.columns:
            sdf_left = sdf_left.withColumnRenamed(col, col + lsuffix)
            sdf_right = sdf_right.withColumnRenamed(col, col + rsuffix)
    geometry_left = f"left.geometry{lsuffix}"
    geometry_right = f"right.geometry{rsuffix}"
    # Add to SQL
    sdf_left.createOrReplaceTempView("left")
    sdf_right.createOrReplaceTempView("right")
    # Join
    if distance == 0:
        sdf = spark.sql(
            f"""
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Intersects({geometry_left}, {geometry_right})
    """
        )
    elif distance > 0:
        sdf = spark.sql(
            f"""
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Distance({geometry_left}, {geometry_right}) < {distance}
    """
        )
    else:
        raise TypeError(f"distance should be positive real: {distance}")
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf


def make_parcel_geometries(parcelsDF: SparkDataFrame) -> SparkDataFrame:
    # Get permieter of parcel as a LineString, then buffer
    parcelsDF = parcelsDF.withColumn(
        "parcel_geom", F.expr("ST_MakeValid(ST_GeomFromWKB(wkb_geometry))")
    )
    parcelsDF = parcelsDF.withColumn(
        "perimeter_geom", F.expr("ST_MakeValid(ST_Boundary(parcel_geom))")
    )
    parcelsDF = parcelsDF.withColumn("perimeter_length", F.expr("ST_Length(perimeter_geom)"))

    return parcelsDF


def join_trees_to_features_and_count_trees(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    featuresDF: SparkDataFrame,
    buffers: List,
    geoFunctions: List[Tuple],
    double_count: bool,
    num_partitions: Optional[int] = 200,
) -> Tuple:
    """
    Intersects tree coordinates with feature geometries and counts the number
    of trees within features on each parcel._ID").distinct().count()

    Parameters
    ----------
    spark:      SparkSession
                The spark session

    treesDf:    SparkDataFrame
                The tree detections.

    featuresDF: SparkDataFrame
                Features data

    buffers:    List
                The buffer distances to use

    geoFunctions: List[Tuple]
                 Function used to get metric from geometries., for example either counting the number of gometries within an area or the length of geometries.

    double_count:   bool
                    When true a single tree can be attributed to multiple parcels where that tree intersects with a hedgerow that looks up to multiple parcels.

    Returns
    -------
    (featureTreesDF, featureTreesPerParcelDF, count):   2-tuple of two SparkDataFrames
                                                        In the first position are the individual feature tree detections. This will potentially contain duplicate tree geometries where a tree intersects multiple features.
                                                        In the second position is the aggregated numbers of feature trees per parcel.
    """

    """
    # Join trees with buffered hedgerows to get hedgerow trees
    featureTreesDF = spatial_join(
                        featuresDF,
                        treesDF,
                        spark,
                        partitioning = None,
                        partition_right = False,
                        useIndex = True,
                        considerBoundaryIntersection = False, # returns tree geometries contained within hedgerow geoms (since trees are point geoms using 'True' should produce the same result)
                        calculate_proportion = False,
                        num_partitions = num_partitions
    )
    """
    # Use elm_se.st spatial join function
    featureTreesDF = (
        sjoin(spark, featuresDF, treesDF, lsuffix="_left", rsuffix="_right", distance=0)
        .withColumnRenamed("geometry_right", "geom_right")
        .withColumnRenamed("geometry_left", "geom_left")
    )

    # Sum hedgerow tree counts to the parcel level
    if double_count:
        subset = [
            "SHEET_PARCEL_ID",
            "geom_right",
        ]  # Counts a tree once per parcel but tree can be counted in multiple parcels
    else:
        subset = ["geom_right"]  # Counts each tree once only

    # Run each function with each buffer distance
    # n - name of metric, use as column in dataframe
    # f - the function
    # x - the buffer distance
    prod = list(itertools.product(geoFunctions, buffers))
    funcsWithBuffers = [f(x).alias(n + str(x)) for (n, f), x in prod]

    featureTreesDF = featureTreesDF.drop_duplicates(subset=subset).select(
        "SHEET_PARCEL_ID", "geom_left", "geom_right", *funcsWithBuffers
    )

    # now aggregate count to parcel IDs
    featureTreesPerParcelDF = featureTreesDF.groupby("SHEET_PARCEL_ID").agg(
        *[F.sum(n + str(x)).alias(n + str(x)) for (n, f), x in prod]
    )

    return featureTreesDF, featureTreesPerParcelDF


def feature_tree_and_parcel_counts(
    featuresDF: SparkDataFrame,
    featureTreesDF: SparkDataFrame,
    featureTreesPerParcelDF: SparkDataFrame,
    bufferDistances: List[str],
    featureNames: List[str],
) -> Dict:
    """
    Get counts of feature trees and parcels with features and feature trees. Useful for reporting and testing.
    """
    counts = {}
    for featureName in featureNames:
        counts[featureName] = {}
        for b in bufferDistances:
            c = featureName + str(b)

            bufferFeatureTreesDF = featureTreesDF.filter(F.col(c) == 1)

            nFeatureTrees = bufferFeatureTreesDF.drop_duplicates(
                subset=["geom_right"]
            ).count()  # total number of trees intersecting with this feature

            df = featureTreesPerParcelDF.select(F.sum(c).alias("tot_parcel_ftrees")).toPandas()
            nFeatureParcelTrees = df["tot_parcel_ftrees"].values[
                0
            ]  # total number of trees intersecting with this feature attributed to parcels - can contain duplicate trees)

            nFeatureParcels = (
                featuresDF
                # .dropna(subset=featureName+str(c))
                .select("SHEET_PARCEL_ID")
                .distinct()
                .count()  # Number of parcels with features
            )
            nFeatureTreeParcels = (
                bufferFeatureTreesDF.select("SHEET_PARCEL_ID").distinct().count()
            )  # parcels with feature trees

            counts[featureName][b] = {
                "nFeatureTrees": nFeatureTrees,
                "nFeatureParcelTrees": nFeatureParcelTrees,
                "nFeatureParcels": nFeatureParcels,
                "nFeatureTreeParcels": nFeatureTreeParcels,
            }

    return counts


def get_hedgerow_trees_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    hrDF: SparkDataFrame,
    bufferDistances: List[float],
    double_count: Optional[bool] = True,
) -> Tuple:
    """
    Produces the hedgerow trees features.

    Old counts method:

    n_parcels = df_parcels.select("SHEET_PARCEL_ID").distinct().count()

    # Number of parcels with hedgerows but not hedgerow trees
    n_hr_parcels = hrDFr.select("PARCEL_ID").distinct().count()

    # hr parcels with hedgerow trees
    n_hrt_parcels = hrtreesDF.select("PARCEL_ID").distinct().count()

    Parameters
    ----------
    treesDf:    SparkDataFrame
                The tree detections.

    hrDF:       SparkDataFrame
                Hedgerow data

    bufferDistances:    List[float]
                        Distances to buffer hedgerows by. Tree count gets calculated at each buffer distance.

    double_count:   bool
                    When true a single tree can be attributed to multiple parcels where that tree intersects with a hedgerow that looks up to multiple parcels.

    Returns
    -------
    (hrtreesDF, hrtreesPerParcelDF, counts):    3-tuple of two SparkDataFrames and a dictionary.
                                                In the first position are the individual hedgerow tree detections.
                                                In the second position is the aggregated numbers of hedgerow trees per parcel.
                                                In the third position is a dictionary with summary data on the number of parcesl with hedgerows and hedgerow trees
    """
    # Create geometry columns used in spatial join
    global buf
    treesDF = treesDF.withColumn("geometry", F.col("top_point"))
    hrDF = hrDF.withColumn(
        "geometry", buf("geometry_hedge", max(bufferDistances))
    )  # buffer features by the maximum buffer distance so that join includes all potential trees.

    featureName = "hrtrees_count"
    global tree_count  # Function used to count trees
    geoFuncs = [(featureName, tree_count)]
    hrtreesDF, hrtreesPerParcelDF = join_trees_to_features_and_count_trees(
        spark,
        treesDF,
        hrDF,
        bufferDistances,
        geoFuncs,
        double_count,
    )
    counts = {}
    '''
    counts = feature_tree_and_parcel_counts(
        hrDF, hrtreesDF, hrtreesPerParcelDF, bufferDistances, [featureName]
    )

    # 13,996,711 - with old params - ws=4 hmin=3 buffer=5
    # 41,632,825 - with new method - ws=2 hmin=2.5 buffer=2
    # Get count of trees when duplicates are not removed.

    report_buffer_distance = 2

    feature_counts = counts[featureName]

    nHRTreesDup = hrtreesDF.filter(F.col(featureName + str(report_buffer_distance)) == 1).count()
    LOG.info(
        f"Number of hedgerow trees (number without duplicate geoms dropped): {feature_counts[report_buffer_distance]['nFeatureTrees']} ({nHRTreesDup})"
    )
    LOG.info(
        f"Number of hedgerow trees in parcels: {feature_counts[report_buffer_distance]['nFeatureParcelTrees']}"
    )
    LOG.info("\n")
    LOG.info(
        f"Number of parcels w hedgerows: {feature_counts[report_buffer_distance]['nFeatureParcels']}"
    )
    LOG.info(
        f"Number of parcels w hedgerow trees: {feature_counts[report_buffer_distance]['nFeatureTreeParcels']}"
    )
    '''

    return hrtreesDF, hrtreesPerParcelDF, counts


def get_waterbody_trees_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    wbDF: SparkDataFrame,
    bufferDistances: List[float],
    waterbodies_to_exclude: Optional[list] = ["Sea", "Named Area Of Sea", "Unknown Or No Physical Feature", "Swimming Pool", "Paddling Pool", "Named Estuary"],
    double_count: Optional[bool] = True,
) -> Tuple:
    """
    Produces the riparian trees features.

    Parameters
    ----------
    treesDf:    SparkDataFrame
                The tree detections.

    wbDF:       SparkDataFrame
                Waterbodies data

    double_count:   bool
                    When true a single tree can be attributed to multiple parcels where that tree intersects with a hedgerow that looks up to multiple parcels.

    Returns
    -------
    (wbTreesDF, wbTreesPerParcelDF, counts):    3-tuple of two SparkDataFrames and a dictionary.
                                                In the first position are the individual hedgerow tree detections.
                                                In the second position is the aggregated numbers of hedgerow trees per parcel.
                                                In the third position is a dictionary with summary data on the number of parcesl with hedgerows and hedgerow trees
    """

    wbDF = wbDF.filter(
        f"""description not in ({",".join("'{}'".format(i) for i in waterbodies_to_exclude)})"""
    )

    nWB = wbDF.count()
    num_partitions = 200
    if nWB < num_partitions * 2:
        num_partitions = int(nWB / 2)

    # Set geometries to use for intersection
    global buf
    treesDF = treesDF.withColumn("geometry", F.col("top_point"))
    wbDF = wbDF.withColumn(
        "geometry", buf("geometry_water", max(bufferDistances))
    )  # buffer features by the maximum buffer distance so that join includes all potential trees.

    featureName = "wbtrees_count"
    global tree_count
    geoFuncs = [(featureName, tree_count)]
    wbTreesDF, wbTreesPerParcelDF = join_trees_to_features_and_count_trees(
        spark,
        treesDF,
        wbDF,
        bufferDistances,
        geoFuncs,
        double_count,
        num_partitions=num_partitions,
    )

    counts = feature_tree_and_parcel_counts(
        wbDF, wbTreesDF, wbTreesPerParcelDF, bufferDistances, [featureName]
    )

    feature_counts = counts[featureName]
    report_buffer_distance = 2
    LOG.info(
        f"Number of waterbody trees: {feature_counts[report_buffer_distance]['nFeatureTrees']}"
    )
    LOG.info(
        f"Number of waterbody trees in parcels: {feature_counts[report_buffer_distance]['nFeatureParcelTrees']}"
    )
    LOG.info("\n")
    LOG.info(
        f"Number of parcels w waterbodies: {feature_counts[report_buffer_distance]['nFeatureParcels']}"
    )
    LOG.info(
        f"Number of parcels w waterbody trees: {feature_counts[report_buffer_distance]['nFeatureTreeParcels']}"
    )

    return wbTreesDF, wbTreesPerParcelDF, counts


def get_perimeter_trees_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    parcelsDF: SparkDataFrame,
    bufferDistances: List[float],
    double_count: Optional[bool] = True,
) -> Tuple[SparkDataFrame]:
    """
    Produces the parcel permimiter trees features.

    Input SparkDataFrames are expected to have geometry fields, produced with the

    Multiple ways to link trees to parcel perimiters:
        1. Intersect tree coordinate with buffered parcel perimeter
        2. Intersect tree crown geometry with non-buffered perimeter
        3. Intersect tree crown geometry with buffered perimeter

    This function uses option 1 for consistency with hedgerow trees method.

    Parameters
    ----------
    treesDf:    SparkDataFrame
                The tree detections.

    parcelsDF:  SparkDataFrame
                Parcels data

    parcelBufferDistances: List[float]
                           Distances to buffer parcel perimeter by when calculating parcel interior

    double_count:   bool
                    When true a single tree can be attributed to multiple parcels where that tree intersects with multiple perimeters.

    Returns
    -------
    SparkDataFrame: Counts of perimeter trees and length of parcel permieter intersected by tree crowns per parcel.
    """
    # Set geometries to use in spatial join
    global buf
    treesDF = treesDF.withColumn("geometry", F.col("top_point"))
    parcelsDF = parcelsDF.withColumn(
        "geometry", buf("perimeter_geom", max(bufferDistances))
    )  # buffer features by the maximum buffer distance so that join includes all potential trees.

    global tree_count
    global tree_intersection_length
    lenFeatureName = "crown_perim_length"
    countFeatureName = "perim_trees_count"
    geoFuncs = [(countFeatureName, tree_count), (lenFeatureName, tree_intersection_length)]
    perimTreesDF, perimTreesPerParcelDF = join_trees_to_features_and_count_trees(
        spark,
        treesDF,
        parcelsDF,
        bufferDistances,
        geoFuncs,
        double_count,
    )

    _ = feature_tree_and_parcel_counts(
        parcelsDF,
        perimTreesDF,
        perimTreesPerParcelDF,
        bufferDistances,
        [countFeatureName, lenFeatureName],
    )

    report_buffer_distance = 2
    nTreeParcels = (
        perimTreesPerParcelDF.dropna(subset=countFeatureName + str(report_buffer_distance))
        .select("SHEET_PARCEL_ID")
        .distinct()
        .count()  # Number of parcels with trees in perimeter
    )
    LOG.info(f"Number of parcels w perimeter trees: {nTreeParcels}")

    return perimTreesPerParcelDF


def get_interior_trees_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    parcelsDF: SparkDataFrame,
    bufferDistances: List[float],
) -> Tuple[SparkDataFrame]:
    """
    Produces the parcel interior trees features. This is the number of trees in the interior of the parcel,
    defined as the differece between the parcel geometry and the buffered parcel perimeter.

    Parameters
    ----------
    spark:      SparkSession

    treesDf:    SparkDataFrame
                The tree detections.

    parcelsDF:  SparkDataFrame
                Parcels data

    bufferDistances: List[float]
                           Distances to buffer parcel perimeter by when calculating parcel interior

    Returns
    -------
    SparkDataFrame: Counts of interior trees per parcel.
    """

    # Set which geometries to use in spatial join
    treesDF = treesDF.withColumn("geometry", F.col("top_point"))  # tree top coordinate
    parcelsDF = parcelsDF.withColumn(
        "geometry", F.col("parcel_geom")
    )  # parcel interior, with buffered perimeter removed

    interior_tree_count = lambda x: F.expr(
        f"""
                                            CAST(
                                                ST_Intersects(
                                                    ST_MakeValid(ST_Difference(parcel_geom, ST_MakeValid(ST_Buffer(perimeter_geom, {x})))),
                                                    ST_MakeValid(geom_right)
                                                    ) AS INTEGER
                                                )as c{x}"""
    )

    featureName = "int_trees_count"
    double_count = False
    geoFuncs = [(featureName, interior_tree_count)]
    iTreesDF, intTreesPerParcelDF = join_trees_to_features_and_count_trees(
        spark,
        treesDF,
        parcelsDF,
        bufferDistances,
        geoFuncs,
        double_count,
    )

    # Log some results
    report_buffer_distance = 2
    nIntTreeParcels = (
        intTreesPerParcelDF.dropna(subset=featureName + str(report_buffer_distance))
        .filter(F.col(featureName + str(report_buffer_distance)) > 0)
        .select("SHEET_PARCEL_ID")
        .distinct()
        .count()  # Number of parcels with trees in interior
    )
    LOG.info(f"Number of parcels w interior trees: {nIntTreeParcels}")

    return intTreesPerParcelDF


def get_parcel_perimeter_and_interior_tree_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    parcelsDF: SparkDataFrame,
    parcel_buffer_distance: float,
    double_count: Optional[bool] = False,
) -> SparkDataFrame:
    """
    Produces parcel trees features comprinsing counts of interior and perimeter trees and the perimieter length
    with trees in (based on tree crowns)

    Input SparkDataFrames are expected to have been read from file meaning that geometry objects need to be created.

    Parameters
    ----------
    treesDf:    SparkDataFrame
                The tree detections.

    parcelsDF:  SparkDataFrame
                Parcels data

    parcel_buffer_distance: float
                            The distance in meters to buffer parcel perimeter geometries by.

    Returns
    -------
    SparkDataFrame: Perimeter and interior tree statistics per parcel.
    """

    # Create geometries
    parcelsDF = make_parcel_geometries(parcelsDF, parcel_buffer_distance)
    treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))

    # Create unique parcel id
    parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID", "PARCEL_ID"))

    parcelPerimTreesDF = get_perimeter_trees_features(
        spark, treesDF, parcelsDF, double_count=double_count
    )
    parcelInterTreesDF = get_interior_trees_features(spark, treesDF, parcelsDF)

    # Combine datasets
    pDF = parcelsDF.select(
        "SHEET_ID", "PARCEL_ID", F.col("SHEET_PARCEL_ID").alias("SPID"), "perimeter_length"
    )
    parcelTreeCountsDF = (
        pDF.join(parcelPerimTreesDF, pDF.SPID == parcelPerimTreesDF.SHEET_PARCEL_ID, "left")
        .drop("SHEET_PARCEL_ID")
        .join(parcelInterTreesDF, pDF.SPID == parcelInterTreesDF.SHEET_PARCEL_ID, "left")
        .drop("SHEET_PARCEL_ID")
        .fillna(0, subset=["perim_trees_count", "perim_trees_length", "int_trees_count"])
        .withColumn("SHEET_PARCEL_ID", F.col("SPID"))
        .drop("SPID")
    )

    nRows = parcelTreeCountsDF.count()
    LOG.info(f"Number of rows in parcel tree features DF: {nRows}")

    nParcels = parcelsDF.select("SHEET_PARCEL_ID").distinct().count()  # All parcels
    LOG.info(f"Number of parcels: {nParcels}")

    # Should be no duplicated parcels
    nP = parcelTreeCountsDF.select("SHEET_PARCEL_ID").count()
    assert nP == nParcels

    return parcelTreeCountsDF


def get_parcel_tree_features(
    spark: SparkSession,
    treesDF: SparkDataFrame,
    parcelsDF: SparkDataFrame,
    hrDF: SparkDataFrame,
    wbDF: SparkDataFrame,
    parcelBufferDistances: List[float],
    hedgerowBufferDistances: List[float],
    waterbodyBufferDistances: List[float],
    double_count: Optional[bool] = False,
) -> SparkDataFrame:
    """
    Produces parcel trees features - counts of hedgerow trees, counts of waterbody trees, counts of interior trees, counts of perimeter trees and the perimieter length
    covered by tree crowns.

    Input SparkDataFrames are expected to have been read from file meaning that geometry objects need to be created.

    Parameters
    ----------
    spark:      SparkSession

    treesDf:    SparkDataFrame
                The tree detections.

    parcelsDF:  SparkDataFrame
                Parcels data

    hrDF:       SparkDataFrame
                Hedgerows data

    wbDF:       SparkDataFrame
                Waterbodies data

    parcelBufferDistance: List[float]
                          List of distances in meters to buffer parcel perimeters when producing perimeter and interior tree intersections.

    hedgerowBufferDistances: List[float]
                             List of distances in meters to buffer hedgerow geometries by.

    waterbodyBufferDistances: List[float]
                              List of distances in meters to buffer waterbody geometries by.

    Returns
    -------
    SparkDataFrame: Tree statistics per parcel.
    """

    # Create geometries
    parcelsDF = make_parcel_geometries(parcelsDF)
    treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))
    hrDF = hrDF.withColumn("wkb", F.col("geometry")).withColumn(
        "geometry_hedge", F.expr("ST_MakeValid(ST_GeomFromWKB(wkb))")
    )

    # Create unique parcel id
    parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID", "PARCEL_ID"))
    hrDF = hrDF.withColumn(
        "SHEET_PARCEL_ID", F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID")
    )
    '''
    wbDF = wbDF.withColumn("SHEET_PARCEL_ID", F.col("id_parcel"))
    '''
    parcelPerimTreesDF = get_perimeter_trees_features(
        spark, treesDF, parcelsDF, parcelBufferDistances, double_count=double_count
    )
    parcelInterTreesDF = get_interior_trees_features(
        spark, treesDF, parcelsDF, parcelBufferDistances
    )

    hrtreesDF, hrtreesPerParcelDF, hrCounts = get_hedgerow_trees_features(
        spark, treesDF, hrDF, hedgerowBufferDistances, double_count=double_count
    )
    '''
    wbTreesDF, wbTreesPerParcelDF, wbCounts = get_waterbody_trees_features(
        spark, treesDF, wbDF, waterbodyBufferDistances, double_count=double_count
    )
    '''

    # Get column names that contain tree metrics
    allFeatureNames = []
    for df in [parcelInterTreesDF,
               hrtreesPerParcelDF, 
               #wbTreesPerParcelDF
               ]:
        allFeatureNames += [i for i in df.columns if i != "SHEET_PARCEL_ID"]

    # Combine datasets
    pDF = parcelsDF.select(
        "SHEET_ID", "PARCEL_ID", F.col("SHEET_PARCEL_ID").alias("SPID"), "perimeter_length"
    )
    parcelTreeCountsDF = (
        pDF.join(hrtreesPerParcelDF, pDF.SPID == hrtreesPerParcelDF.SHEET_PARCEL_ID, "left")
        .drop("SHEET_PARCEL_ID")
        #.join(wbTreesPerParcelDF, pDF.SPID == wbTreesPerParcelDF.SHEET_PARCEL_ID, "left")
        #.drop("SHEET_PARCEL_ID")
        .join(parcelPerimTreesDF, pDF.SPID == parcelPerimTreesDF.SHEET_PARCEL_ID, "left")
        .drop("SHEET_PARCEL_ID")
        .join(parcelInterTreesDF, pDF.SPID == parcelInterTreesDF.SHEET_PARCEL_ID, "left")
        .drop("SHEET_PARCEL_ID")
        #.fillna(0, subset=["perim_trees_count", "perim_trees_length", "int_trees_count", "hrtrees_count", "wbtrees_count"])
        .fillna(0, subset=allFeatureNames)
        .withColumn("SHEET_PARCEL_ID", F.col("SPID"))
        .drop("SPID")
    )

    nRows = parcelTreeCountsDF.count()
    LOG.info(f"Number of rows in parcel tree features DF: {nRows}")

    nParcels = parcelsDF.select("SHEET_PARCEL_ID").distinct().count()  # All parcels
    LOG.info(f"Number of parcels: {nParcels}")

    # Should be no duplicated parcels
    nP = parcelTreeCountsDF.select("SHEET_PARCEL_ID").count()
    assert nP == nParcels

    return parcelTreeCountsDF
