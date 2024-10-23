from shapely import LineString, MultiLineString, MultiPoint, Point, segmentize


def linear_to_multiline(geometry):
    if geometry.geom_type == "LineString":
        geometry = MultiLineString([geometry])
    return geometry


def linear_to_coords(geometry):
    geometry = linear_to_multiline(geometry)
    for geom in geometry.geoms:
        yield from geom.coords


def linear_to_multipoint(geometry):
    return MultiPoint(list(linear_to_coords(geometry)))


def closest_point_index(point: Point, points: MultiPoint) -> int:
    """Gets the index of the closest Point of a MultiPoint from a Point.
    Without constructing an RTree.
    """
    distances = [point.distance(p) for p in points.geoms]
    return distances.index(min(distances))


def segmentise_with_tolerance(geometry: LineString, tolerance: float = 10, length: float = 50) -> MultiLineString:
    r"""Segments a LineString into smaller segments based on tolerance and length limit.
    Uses segment on each vertex methodology, but adds a tolerance to create longer segments.
    Arguments:
        geometry (LineString): The input LineString geometry.
        tolerance (float): The tolerance used for simplification.
        length (float): The maximum length of each segment.
    Returns:
        MultiLineString: The segmented LineString.
    Visual Demonstration:
    ```
    *----.     .--*  Original Input
         |    /      LineString, "." means interior node, "*" means end node.
         .---.

    *----*     *--*  Simplify and segmented at nodes.
          \   /      MultiLineString, with different geometry.
           *

    *----.     .--*  Segmentation at length
         |    /      MultiLineString, but split not at a node.
         .-*-.

    *----*    .--*  Segment with tolerance
         |    /     MultiLineString, returns the original geometry, but split at nodes and major corners.
         *---.
    ```
    """
    original = linear_to_multipoint(segmentize(geometry, length))
    simplified = linear_to_multipoint(segmentize(geometry.simplify(tolerance), length))
    indices = [0] + [closest_point_index(point, original) for point in simplified.geoms] + [len(original.geoms) - 1]
    indices = sorted(list(set(indices)))
    slices = zip(indices[:-1], indices[1:])
    return MultiLineString([original.geoms[i : j + 1].geoms for i, j in slices])
