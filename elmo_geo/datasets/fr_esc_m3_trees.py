"""Ecological Site Classification M3 tree suitability and carbon sequestration outputs.

These datasets are the outputs of Forest Research's Ecolocial Site Classification (ESC)[^1]
tree suitability and yield class models. They provide suitability suitability scores for different
tree species under four Representative Concentration Pathway (RCP) climate scenarios and four
time periods. These outputs are reported for 1km Ordnance Survey GB grid cells.

This module also defines derived datasets that aggregate the 1km grid ESC trees outputs to RPA
parcels.

[^1] [Forest Research - Ecological Site Classification](https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification)
"""

from elmo_geo.etl import SourceDataset

source_dir = "/Workspace/Users/obi.thompsonsargoni@defra.gov.uk/M3_Trees/"

esc_kwargs = {
    "level0": "bronze",
    "level1": "forest_research",
    "restricted": False,
    "is_geo": False,
}
#
# Broadleaved
#
esc_broadleaved_26_raw = SourceDataset(
    **esc_kwargs,
    name="esc_broadleaved_26_raw",
    source_path=source_dir + "EVAST_M3_native_broadleaved_rcp26.csv",
)
"""ESC broadleaved dataset for the RCP 2.6 scenario."""

esc_broadleaved_45_raw = SourceDataset(
    **esc_kwargs,
    name="esc_broadleaved_45_raw",
    source_path=source_dir + "EVAST_M3_native_broadleaved_rcp45.csv",
)
"""ESC broadleaved dataset for the RCP 4.5 scenario."""

esc_broadleaved_60_raw = SourceDataset(
    **esc_kwargs,
    name="esc_broadleaved_60_raw",
    source_path=source_dir + "EVAST_M3_native_broadleaved_rcp60.csv",
)
"""ESC broadleaved dataset for the RCP 6.0 scenario."""

esc_broadleaved_85_raw = SourceDataset(
    **esc_kwargs,
    name="esc_broadleaved_85_raw",
    source_path=source_dir + "EVAST_M3_native_broadleaved_rcp85.csv",
)
"""ESC broadleaved dataset for the RCP 8.5 scenario."""

#
# Riparian
#
esc_riparian_26_raw = SourceDataset(
    **esc_kwargs,
    name="esc_riparian_26_raw",
    source_path=source_dir + "EVAST_M3_native_riparian_rcp26.csv",
)
"""ESC riparian dataset for the RCP 2.6 scenario."""

esc_riparian_45_raw = SourceDataset(
    **esc_kwargs,
    name="esc_riparian_45_raw",
    source_path=source_dir + "EVAST_M3_native_riparian_rcp45.csv",
)
"""ESC riparian dataset for the RCP 4.5 scenario."""

esc_riparian_60_raw = SourceDataset(
    **esc_kwargs,
    name="esc_riparian_60_raw",
    source_path=source_dir + "EVAST_M3_native_riparian_rcp60.csv",
)
"""ESC riparian dataset for the RCP 6.0 scenario."""

esc_riparian_85_raw = SourceDataset(
    **esc_kwargs,
    name="esc_riparian_85_raw",
    source_path=source_dir + "EVAST_M3_native_riparian_rcp85.csv",
)
"""ESC riparian dataset for the RCP 8.5 scenario."""

#
# Productive conifer
#
esc_productive_conifer_26_raw = SourceDataset(
    **esc_kwargs,
    name="esc_productive_conifer_26_raw",
    source_path=source_dir + "EVAST_M3_native_productive_conifer_rcp26.csv",
)
"""ESC productive conifer dataset for the RCP 2.6 scenario."""

esc_productive_conifer_45_raw = SourceDataset(
    **esc_kwargs,
    name="esc_productive_conifer_45_raw",
    source_path=source_dir + "EVAST_M3_native_productive_conifer_rcp45.csv",
)
"""ESC productive conifer dataset for the RCP 4.5 scenario."""

esc_productive_conifer_60_raw = SourceDataset(
    **esc_kwargs,
    name="esc_productive_conifer_60_raw",
    source_path=source_dir + "EVAST_M3_native_productive_conifer_rcp60.csv",
)
"""ESC productive conifer dataset for the RCP 6.0 scenario."""

esc_productive_conifer_85_raw = SourceDataset(
    **esc_kwargs,
    name="esc_productive_conifer_85_raw",
    source_path=source_dir + "EVAST_M3_native_productive_conifer_rcp85.csv",
)
"""ESC productive conifer dataset for the RCP 8.5 scenario."""

#
# Wood pasture
#
esc_wood_pasture_26_raw = SourceDataset(
    **esc_kwargs,
    name="esc_wood_pasture_26_raw",
    source_path=source_dir + "EVAST_M3_native_wood_pasture_rcp26.csv",
)
"""ESC wood pasture dataset for the RCP 2.6 scenario."""

esc_wood_pasture_45_raw = SourceDataset(
    **esc_kwargs,
    name="esc_wood_pasture_45_raw",
    source_path=source_dir + "EVAST_M3_native_wood_pasture_rcp45.csv",
)
"""ESC wood pasture dataset for the RCP 4.5 scenario."""

esc_wood_pasture_60_raw = SourceDataset(
    **esc_kwargs,
    name="esc_wood_pasture_60_raw",
    source_path=source_dir + "EVAST_M3_native_wood_pasture_rcp60.csv",
)
"""ESC wood pasture dataset for the RCP 6.0 scenario."""

esc_wood_pasture_85_raw = SourceDataset(
    **esc_kwargs,
    name="esc_wood_pasture_85_raw",
    source_path=source_dir + "EVAST_M3_native_wood_pasture_rcp85.csv",
)
"""ESC wood pasture dataset for the RCP 8.5 scenario."""

esc_source_datasets = [
    esc_broadleaved_26_raw,
    esc_broadleaved_45_raw,
    esc_broadleaved_60_raw,
    esc_broadleaved_85_raw,
    esc_riparian_26_raw,
    esc_riparian_45_raw,
    esc_riparian_60_raw,
    esc_riparian_85_raw,
    esc_productive_conifer_26_raw,
    esc_productive_conifer_45_raw,
    esc_productive_conifer_60_raw,
    esc_productive_conifer_85_raw,
    esc_wood_pasture_26_raw,
    esc_wood_pasture_45_raw,
    esc_wood_pasture_60_raw,
    esc_wood_pasture_85_raw,
]
