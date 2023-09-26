# __name__ = 'elmo_geo'


# from subprocess import run

# requirements = open('../requirements.txt').read().split()
# run(["pip", "install"] + requirements)


from elmo_geo.utils.log import LOG
from elmo_geo.utils import settings
from elmo_geo.utils import types
from elmo_geo.utils import dbr  # spark, dbutils, sc, display
from elmo_geo.utils.register import register

# from elmo_geo.io.ingest import ingest

# from elmo_geo.st.join import join
# from elmo_geo.st.index import index

# from elmo_geo.r import ...

# from elmo_geo.plot import ...
