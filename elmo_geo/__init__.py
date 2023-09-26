__name__ = 'elmo_geo'


import subprocess

requirements = open('../requirements.txt').read().split()
subprocess.run(["pip", "install"] + requirements)


from elmo_geo.utils.log import LOG
from elmo_geo.utils.register import register
