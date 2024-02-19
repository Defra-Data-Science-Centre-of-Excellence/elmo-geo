from setuptools import find_packages, setup

__title__ = "elmo-geo"
__description__ = "Environmental Land Management Geospatial Analysis"
__version__ = "0.1"

setup(
    name=__title__,
    description=__description__,
    version=__version__,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=open("requirements.txt").split(),
)
