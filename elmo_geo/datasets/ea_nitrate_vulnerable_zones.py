"""
This dataset identifies Nitrate Vulnerable Zones for implementation in 2021.

The designations are made by the Secretary of State for the purposes of the Nitrate Pollution Prevention Regulations 2015 (England).
This dataset supersedes the previous dataset ‘Nitrate Vulnerable Zones (NVZ) 2017 – Combined (Final Designations)’.

Nitrate Vulnerable Zones (NVZs) are areas designated as being at risk from agricultural nitrate pollution.
Waters are defined within the Nitrates Directive and Nitrate Regulations as polluted if they:
 - contain or could contain, if preventative action is not taken, nitrate concentrations greater than 50mg/l
 - are eutrophic, or become eutrophic, if preventative action is not taken.
"""


from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


class EANVZRaw(DataFrameModel):
    """Model for the Nitrate Vulnerable Zones (NVZ) dataset

    Attributes:
      nvz_name: Name of zone
      nvz_id: zone id number
      nvz_type: type of designation
      nvz_2017: 2017 designation
      nvz_2021: 2021 designation
      nvz_status: status of designation, all are 'Existing'
      geometry: Geospatial polygons in EPSG:27700
    """

    nvz_name: str = Field()
    nvz_id: str = Field()
    nvz_type: str = Field()
    nvz_status: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ea_nvz_raw = SourceDataset(
    name="ea_nvz_raw",
    medallion="bronze",
    source="ea",
    model=EANVZRaw,
    restricted=False,
    source_path="/dbfs/FileStore/elmo-geo-downloads/Nitrate_Vulnerable_Zones_2021_Designations.shp",
)
"""The raw geospaital dataset for the environment agency nitrate vulnerable zones
"""


class EANVZParcels(DataFrameModel):
    """Model for the Nitrate Vulnerable Zones (NVZ) dataset joined with Rural Payment Agency parcel dataset.

    Attributes:
      id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419
      nvz_type: nvz designation type 
      nvz_2017: 2017 designation
      nvz_2021: 2021 designation
      proportion: The proportion of the parcel that intersects with the NVZ
    """

    id_parcel: str = Field()
    nvz_type: str = Field()
    nvz_2017: str = Field()
    nvz_2021: str = Field()
    proportion: float = Field(ge=0, le=1)


ea_nvz_parcels = DerivedDataset(
    is_geo=False,
    name="ea_nvz_parcels",
    medallion="silver",
    source="ea",
    restricted=False,
    func=partial(sjoin_parcel_proportion, columns=["nvz_type", "nvz_2017", "nvz_2021"]),
    dependencies=[reference_parcels, ea_nvz_raw],
    model=EANVZParcels,
)
"""Indicates the proportion of each parcel intersected by the Nitrate Vulnerable zones dataset.
"""
