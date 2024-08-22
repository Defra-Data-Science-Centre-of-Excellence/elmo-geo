"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo import register
from elmo_geo.utils.log import LOG

from .commons import commons_parcels, commons_raw  # TODO: link, is_conclusive, gpkg
from .defra_alc import alc_parcels, alc_raw  # TODO: link
from .defra_flood_risk_areas import flood_risk_areas_parcels, flood_risk_areas_raw  # TODO: link
from .fc_ewco import (
    ewco_nature_recovery_priority_habitat,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
)
from .fc_woodland_sensitivity import (
    sfi_agroforestry,
    sfi_agroforestry_parcels,
    sfi_agroforestry_raw,
    woodland_creation_sensitivity,
    woodland_creation_sensitivity_parcels,
    woodland_creation_sensitivity_raw,
    woodland_creation_sensitivity_var1,
    woodland_creation_sensitivity_var1_parcels,
    woodland_creation_sensitivity_var1_raw,
    woodland_creation_sensitivity_var2,
    woodland_creation_sensitivity_var2_parcels,
    woodland_creation_sensitivity_var2_raw,
    woodland_creation_sensitivity_var3,
    woodland_creation_sensitivity_var3_parcels,
    woodland_creation_sensitivity_var3_raw,
)
from .hedges import rpa_hedges_parcels, rpa_hedges_raw  # TODO: link, parcel_boundary_segments
from .living_england import living_england_habitat_map_phase_4_parcel, living_england_habitat_map_phase_4_raw  # TODO: link
from .moor import moorline_parcel, moorline_raw  # TODO: link, gpkg
from .ons import (  # TODO: links*2, itl3
    bua_parcels,
    bua_raw,
    country_parcels,
    country_raw,
    cua_parcels,
    cua_raw,
    itl2_parcels,
    itl2_raw,
    lad_parcels,
    lad_raw,
    region_parcels,
    region_raw,
    ward_parcels,
    ward_raw,
)
from .os import *  # noqa TODO: blocker geoparquet
from .osm import osm_parcel, osm_raw, osm_tidy  # TODO: link, fn_osm_tidy
from .peat import peaty_soils_parcels, peaty_soils_raw  # TODO: link
from .protected_landscapes import (  # TODO: link, test_combine
    national_landscapes_raw,
    national_parks_raw,
    protected_landscapes_parcels,
    protected_landscapes_tidy,
)
from .rpa_reference_parcels import reference_parcels  # TODO: link

# TODO: test they work


catalogue = [
    alc_parcels,
    alc_raw,
    bua_parcels,
    bua_raw,
    commons_parcels,
    commons_raw,
    country_parcels,
    country_raw,
    cua_parcels,
    cua_raw,
    flood_risk_areas_parcels,
    flood_risk_areas_raw,
    ewco_nature_recovery_priority_habitat,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    itl2_parcels,
    itl2_raw,
    living_england_habitat_map_phase_4_parcel,
    living_england_habitat_map_phase_4_raw,
    lad_parcels,
    lad_raw,
    moorline_parcel,
    moorline_raw,
    reference_parcels,
    region_parcels,
    region_raw,
    sfi_agroforestry,
    sfi_agroforestry_parcels,
    sfi_agroforestry_raw,
    ward_parcels,
    ward_raw,
    woodland_creation_sensitivity,
    woodland_creation_sensitivity_parcels,
    woodland_creation_sensitivity_raw,
    woodland_creation_sensitivity_var1,
    woodland_creation_sensitivity_var1_parcels,
    woodland_creation_sensitivity_var1_raw,
    woodland_creation_sensitivity_var2,
    woodland_creation_sensitivity_var2_parcels,
    woodland_creation_sensitivity_var2_raw,
    woodland_creation_sensitivity_var3,
    woodland_creation_sensitivity_var3_parcels,
    woodland_creation_sensitivity_var3_raw,
]
"""List of datasets in `elmo_geo`."""


def write_catalogue_json():
    "Write the catalogue as a json."
    register()
    with open("data/catalogue.json", "w") as f:
        f.write(json.dumps({dataset.name: dataset.dict for dataset in catalogue}, indent=4))


def destroy_datasets():
    """Destroy all datasets in the catalogue.

    Warning:
        Datasets may take long time to rebuild the next time you need them.
    """
    register()
    for dataset in catalogue:
        dataset.destroy()


def main():
    """Run the whole ETL to build any missing datasets and refresh any stale ones."""
    register()
    LOG.info("Refreshing all datasets...")
    for dataset in catalogue:
        if not dataset.is_fresh:
            dataset.refresh()
    LOG.info("All datasets are up to date.")


__all__ = ["catalogue", "destroy_datasets", "write_catalogue_json"]


if __name__ == "__main__":
    main()
