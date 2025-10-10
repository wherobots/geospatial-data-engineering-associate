#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
from pyspark.sql.functions import col, when, expr
from sedona.sql.st_functions import ST_IsValid, ST_IsValidReason, ST_MakeValid
from pyspark.sql import DataFrame
import urllib.request
import json


# In[ ]:


config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


stac_df = sedona.read.format("stac").load(
    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a"
)

stac_df.printSchema()
stac_df.select("id", "datetime", "geometry", "collection").show(5, truncate=False)


# You can control how many items to load, how requests are batched, and how partitions are distributed.

# In[ ]:


df = sedona.read.format("stac") \
            .option("itemsLimitMax", "1000")\
            .option("itemsLimitPerRequest", "50")\
            .option("itemsLoadProcessReportThreshold", "500000")\
            .load("https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a")


# In[ ]:


# Create a new Havasu (Iceberg) database

database = 'gde_bronze'

sedona.sql(f'CREATE DATABASE IF NOT EXISTS org_catalog.{database}')


# In[ ]:


geotiff_path = "s3://wherobots-examples/data/ghs_population/GHS_POP_E1975_GLOBE_R2023A_4326_3ss_V1_0.tif"

df = sedona.read.format("raster") \
    .option("tileWidth", "512") \
    .option("tileHeight", "512") \
    .option("retile", "true") \
    .load(geotiff_path)

df.writeTo(f"org_catalog.{database}.ghs_population_tiles")


# In[ ]:


df.writeTo(f"org_catalog.{database}.ghs_population_tiles").create()


# In[ ]:


prefix = 's3://wherobots-examples/gdea-course-data/raw-data/'


# In[ ]:


# FEMA Flood Hazard Areas
fld_hazard_area = sedona.read.format('shapefile').load(f'{prefix}' + '53033C_20250330/S_FLD_HAZ_AR.shp')


# In[ ]:


fld_hazard_area.writeTo(f"org_catalog.{database}.fema_flood_zones_bronze").createOrReplace()


# In[ ]:


# King County Generalized Land Use Data
gen_land_use = sedona.read.format('shapefile').load(f'{prefix}' + 'General_Land_Use_Final_Dataset/General_Land_Use_Final_Dataset.shp')


# In[ ]:


gen_land_use.writeTo(f"org_catalog.{database}.gen_land_use_bronze").createOrReplace()


# In[ ]:


# King County Sherrif Patrol Districts
sherrif_districts = sedona.read.format('shapefile').load(f'{prefix}' + 'King_County_Sheriff_Patrol_Districts___patrol_districts_area/King_County_Sheriff_Patrol_Districts___patrol_districts_area.shp')


# In[ ]:


sherrif_districts.writeTo(f"org_catalog.{database}.sherrif_districts_bronze").createOrReplace()


# In[ ]:


# King County Offense Reports
offense_reports = sedona.read.format('csv').load(f'{prefix}' + 'KCSO_Offense_Reports__2020_to_Present_20250923.csv')


# In[ ]:


offense_reports.writeTo(f"org_catalog.{database}.offense_reports_bronze").createOrReplace()


# In[ ]:


# King County Bike Lanes
bike_lanes = sedona.read.format('shapefile').load(f'{prefix}' + 'Metro_Transportation_Network_(TNET)_in_King_County_for_Bicycle_Mode___trans_network_bike_line/Metro_Transportation_Network_(TNET)_in_King_County_for_Bicycle_Mode___trans_network_bike_line.shp')


# In[ ]:


bike_lanes.writeTo(f"org_catalog.{database}.bike_lanes_bronze").createOrReplace()


# In[ ]:


# FEMA National Risk Index
fema_nri = sedona.read.format('shapefile').load(f'{prefix}' + 'NRI_Shapefile_CensusTracts/NRI_Shapefile_CensusTracts.shp')


# In[ ]:


fema_nri.writeTo(f"org_catalog.{database}.fema_nri_bronze").createOrReplace()


# In[ ]:


# King County School Sites
school_sites = sedona.read.format('shapefile').load(f'{prefix}' + 'School_Sites_in_King_County___schsite_point/School_Sites_in_King_County___schsite_point.shp')


# In[ ]:


school_sites.writeTo(f"org_catalog.{database}.school_sites_bronze").createOrReplace()


# In[ ]:


# Schools Report Card
report_card = sedona.read. \
    format('csv'). \
    load(f'{prefix}' + 'Report_Card_Growth_for_2024-25_20250923.csv')


# In[ ]:


report_card.writeTo(f"org_catalog.{database}.report_card_bronze").createOrReplace()


# In[ ]:


# Seismic Hazards
seismic_hazards = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'Seismic_Hazards___seism_area/Seismic_Hazards___seism_area.shp')


# In[ ]:


seismic_hazards.writeTo(f"org_catalog.{database}.seismic_hazards_bronze").createOrReplace()


# In[ ]:


# Census Block Groups
block_groups = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'tl_2024_53_bg/tl_2024_53_bg.shp')


# In[ ]:


block_groups.writeTo(f"org_catalog.{database}.block_groups_bronze").createOrReplace()


# In[ ]:


# Census CSVs
median_age = sedona.read. \
    format('csv'). \
    load(f'{prefix}' + 'ACSDT5Y2023.B01002_2025-09-19T105233/ACSDT5Y2023.B01002-Data.csv')

median_age.writeTo(f"org_catalog.{database}.median_age_bronze").createOrReplace()

total_pop = sedona.read. \
    format('csv'). \
    load(f'{prefix}' + 'ACSDT5Y2023.B01003_2025-09-19T105050/ACSDT5Y2023.B01003-Data.csv')

total_pop.writeTo(f"org_catalog.{database}.total_pop_bronze").createOrReplace()

median_income = sedona.read. \
    format('csv'). \
    load(f'{prefix}' + 'ACSDT5Y2023.B19013_2025-09-19T105253/ACSDT5Y2023.B19013-Data.csv')

total_pop.writeTo(f"org_catalog.{database}.median_income_bronze").createOrReplace()


# In[ ]:


# Tranist Routes
transit_routes = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'Transit_Routes_for_King_County_Metro___transitroute_line/Transit_Routes_for_King_County_Metro___transitroute_line.shp')


# In[ ]:


transit_routes.writeTo(f"org_catalog.{database}.transit_routes_bronze").createOrReplace()


# In[ ]:


# Transit Stops
transit_stops = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'Transit_Stops_for_King_County_Metro___transitstop_point/Transit_Stops_for_King_County_Metro___transitstop_point.shp')


# In[ ]:


transit_stops.writeTo(f"org_catalog.{database}.transit_stops_bronze").createOrReplace()


# In[ ]:


# Water Bodies
water_bodies = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'Waterbodies_with_History_and_Jurisdictional_detail___wtrbdy_det_area/Waterbodies_with_History_and_Jurisdictional_detail___wtrbdy_det_area.shp')


# In[ ]:


water_bodies.writeTo(f"org_catalog.{database}.water_bodies_bronze").createOrReplace()


# In[ ]:


# Wildfire Polygons
wildfires = sedona.read. \
    format('shapefile'). \
    load(f'{prefix}' + 'Wildfires_1878_2019_Polygon_Data/Shapefile/US_Wildfires_1878_2019.shp')


# In[ ]:


wildfires.writeTo(f"org_catalog.{database}.wildfires_bronze").createOrReplace()


# In[ ]:


# Elevation

url = 's3://copernicus-dem-30m/*/*.tif'

elev_df = sedona.read.format("raster").option("retile", "true").load(url) \
.where(
    "RS_Intersects(rast, ST_GeomFromText('POLYGON((-125.0572 48.9964, -120.255 48.9964, -120.255 46.491, -125.0572 46.491, -125.0572 48.9964))'))"
)

# Use the below function to load the global DEM

# elev_df = sedona.read.format("raster").option("retile", "true").load(url)


# In[ ]:


elev_df.writeTo(f"org_catalog.{database}.elevation_bronze").createOrReplace()


# In[ ]:


# Geocoded Schools
schools = sedona.read. \
    format('geojson'). \
    option('mode', 'DROPMALFORMED'). \
    load(f'{prefix}' + 'Washington_State_Public_Schools_GeoCoded.geojson')


# In[ ]:


schools = schools \
    .withColumn("AYPCode", expr("properties['AYPCode']")) \
    .withColumn("CongressionalDistrict", expr("properties['CongressionalDistrict']")) \
    .withColumn("County", expr("properties['County']")) \
    .withColumn("ESDName", expr("properties['ESDName']")) \
    .withColumn("Email", expr("properties['Email']")) \
    .withColumn("GeoCoded_X", expr("properties['GeoCoded_X']")) \
    .withColumn("GeoCoded_Y", expr("properties['GeoCoded_Y']")) \
    .withColumn("GradeCategory", expr("properties['GradeCategory']")) \
    .withColumn("HighestGrade", expr("properties['HighestGrade']")) \
    .withColumn("LEAName", expr("properties['LEAName']")) \
    .withColumn("LegislativeDistrict", expr("properties['LegislativeDistrict']")) \
    .withColumn("LowestGrade", expr("properties['LowestGrade']")) \
    .withColumn("MailingAddress", expr("properties['MailingAddress']")) \
    .withColumn("NCES_X", expr("properties['NCES_X']")) \
    .withColumn("NCES_Y", expr("properties['NCES_Y']")) \
    .withColumn("Phone", expr("properties['Phone']")) \
    .withColumn("Principal", expr("properties['Principal']")) \
    .withColumn("School", expr("properties['School']")) \
    .withColumn("SchoolCategory", expr("properties['SchoolCategory']")) \
    .withColumn("SingleAddress", expr("properties['SingleAddress']")) \
    .drop("properties").drop("type") \
    .drop("_corrupt_record").drop("type") \
    .drop("type").drop("type")


# In[ ]:


schools.writeTo(f"org_catalog.{database}.schools_bronze").createOrReplace()


# In[ ]:




