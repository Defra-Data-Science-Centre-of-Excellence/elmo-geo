# Databricks notebook source

# boundary splitting method...

sdf_info = spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/out/elmo_geo-business_info-2024_01_30.parquet')
sdf_boundary = load_sdf('elmo_geo-boundary')

sdf_hedge = load_sdf('elmo_geo-hedge')
sdf_water = load_sdf('elmo_geo-water')
sdf_wall = load_sdf('elmo_geo-wall')

