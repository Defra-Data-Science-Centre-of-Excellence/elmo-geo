"""Functions for processing geospatial data using GeoVector cluster for ELMO"""

import subprocess

from elmo_geo.utils.log import LOG
from elmo_geo.utils.register import register


requirements = open('../requirements.txt').read().split()
subprocess.run(["pip", "install"] + requirements)
