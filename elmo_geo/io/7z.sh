#!/bin/bash
f="$1"

7z a -v100m -mmt $f.7z $f
