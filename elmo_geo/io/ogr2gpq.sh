#!/bin/bash
f_in=$1
f_out=$2

PATH=$PATH:/databricks/miniconda/bin
TMPDIR=/tmp
PROJ_LIB=/databricks/miniconda/share/proj
OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO

rm -r $f_out
layers=$(ogrinfo -so $f_in | grep -oP '^\d+: \K[^ ]*')
if [ ${#layers[@]} -lt 2 ]; then
    ogr2ogr -f Parquet $f_out $f_in
else
    mkdir -p $f_out
    for layer in $layers; do
        ogr2ogr -f Parquet $f_out/$layer $f_in $layer
    done
fi
