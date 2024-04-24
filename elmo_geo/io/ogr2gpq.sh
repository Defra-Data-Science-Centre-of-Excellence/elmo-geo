#!/bin/bash
f_in=$1
f_out=$2

PATH=$PATH:/databricks/miniconda/bin
TMPDIR=/tmp
PROJ_LIB=/databricks/miniconda/share/proj
OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO


layers=$(ogrinfo -so $f_in | grep -oP '^\d+: \K[^ ]*')
if [ -e $f_out ]; then
    rm -r $f_out
fi
if [ $(echo "$layers" | wc -w) -lt 2 ]; then
    echo $f_out
    ogr2ogr -f Parquet $f_out $f_in
else
    mkdir $f_out
    for layer in $layers; do
        echo $f_out/$layer
        ogr2ogr -f Parquet $f_out/layer=$layer $f_in $layer
    done
fi
