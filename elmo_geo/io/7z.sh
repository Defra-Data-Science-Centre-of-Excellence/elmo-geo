#!/bin/bash
f="$1"

7z a -v100m -mx=9 -mmt -r -o./data_sync/ $f.7z $f
