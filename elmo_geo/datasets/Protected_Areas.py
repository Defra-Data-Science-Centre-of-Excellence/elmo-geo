""" Protected Areas for Conservation identified using guidance written by the JNCC protected areas include:

- Sites of Special Scientific Interest (SSSI)
    Sites of Special Scientific Interest Site (SSSI) Units are divisions of SSSIs based on habitat, tenure and management, and are the basis for recording all information on SSSI Condition and management. They range in Area from 0.004ha up to 18,000ha and only overlap where SSSIs overlap. A SSSI is the land notified as an SSSI under the Wildlife and Countryside Act (1981), as amended. Sites notified under the 1949 Act only are not included in the Data set. SSSI are the finest sites for wildlife and natural features in England, supporting many characteristic, rare and endangered species, habitats and natural features.

- National Nature Reserves

- Special areas of Conservation (SAC)
- Special Protection Area (SPA)
- Ramsar
- Marine Conservation Zones
- Nature Conservation Marine Protected Areas

JNCC guidance available here: https://jncc.gov.uk/our-work/uk-protected-areas/"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(join_parcels, columns=["sssi_name", "id", "condition"])

class NESSSUnitsIRaw(DataFrameModel):
    """model for NE SSSI units dataset.
    Attributes:
       sssi_name:name of the SSSI
       id: reference id for the SSSI unit
       condition: condition of the SSSI unit, the objective is for all to be in favourable condition
       geometry: The parcel geospatial polygons in EPSG:27700.
    """
    sssi_name: str = Field(coerce- True)
    id: float = Field(ge=0, le=1)
    condition: str = Field(coerce- True)
    geometry: Geometry(crs=SRID) = Field(coerce=True, nullable=True)

ne_sssi_units_raw = SourceDataset(
    name="ne_sssi_units_raw",
    Level0="bronze",
    level1="ne",
    model=,NESSSUnitsIRaw
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest_units/format_GEOPARQUET_sites_of_special_scientific_interest_units/LATEST_sites_of_special_scientific_interest_units",
)



