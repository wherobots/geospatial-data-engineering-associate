#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sedona.spark import *
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException

config = SedonaContext.builder() \
    .getOrCreate()

sedona = SedonaContext.create(config)


# # Recommended runtime
# 
# ## Tiny
# 
# # Cost: 
# 
# Cost of executing this over King County.
# 
# - **~$6**
# 
# Cost of executing this over Kirkland.
# 
# - **>$1**

# # NDVI
# 
# Find average NDVI over 500m radius of the house.
# 
# ## **NDVI** Formula
# 
# ($\frac{\mathrm{NIR} - \mathrm{RED}}{\mathrm{NIR} + \mathrm{RED}}$)
# 
# ### Bands used 
# - RED   / B04
# - NIR   / B08
# 

# Make sure to filter out the unnecessary rasters outside of King County.
# 
# 
# ```python
# import wkls
# 
# _aoi = wkls.us.wa.kingcounty.wkt()
# ```

# In[2]:


import wkls

_aoi = wkls.us.wa.kirkland.wkt()


# Trying to enrich the existing `house_sales_silver` table, if not avaliable then falling back to the bronze table.

# In[13]:


try:
    print("**** Trying to load house_sales_silver dataset from gde_silver database **** \n\n")
    house_sales_df = (
        sedona.table(f"org_catalog.gde_silver.house_sales_silver")
                .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}'))")
                .withColumn("geometry_buffer", expr("ST_Buffer(geometry, 500, true)"))
    )
    print("**** house_sales_silver dataset found and loaded. **** \n\n")
except AnalysisException as e:
    print("**** house_sales_silver table doesn't exist, reading the gde_bronze.house_sales_bronze **** \n\n")

    house_sales_df = (
        sedona.table(f"org_catalog.gde_bronze.house_sales_bronze")
                .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}'))")
                .withColumn("geometry_buffer", expr("ST_Buffer(geometry, 500, true)"))
    )


house_sales_df.createOrReplaceTempView("house_sales")


# # Reading in the sentinel 2 dataset using the STAC API

# In[4]:

print("**** Loading Sentinel-2 Data from STAC API **** \n\n")

# Load from STAC datasource
df = (sedona.read.format("stac") 
    .option("itemsLimitMax", "1000") 
    .option("itemsLimitPerRequest", "200") 
    .load("https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a")  # STAC Endpoint
    .where(f"ST_INTERSECTS(geometry, ST_GeomFromWKT('{_aoi}'))")                      # Apply Spatial Filter
    .withColumn("rast_nir", expr("assets.nir.rast"))                                  # Promote the NIR band
    .withColumn("rast_red", expr("assets.red.rast"))                                  # Promote the Red band
    .select("id",                                                                     #--------------------
            expr("date(datetime)"),                                                   #
            col("eo:cloud_cover").alias("cloud_cover"),                               #
            col("grid:code").alias("grid_code"),                                      #
            "geometry",                                                               # Select Attributes
            "rast_red",                                                               #
            "rast_nir",                                                               #
            "platform",                                                               #
            "constellation",                                                          #--------------------
           )
    .filter("cloud_cover <  10")                                                      #Apply the cloud cover filter
    .filter("DATE(datetime) between '2024-06-01' and'2024-07-31'")                    #Apply Date Range Filter
    .orderBy(expr("date(datetime)"))
    .repartition(1_000)
    .cache()
     )
df.createOrReplaceTempView("asset_items")




# ## Stacking the Red and NIR bands into one raster

# In[6]:

print("**** Stacking Red and NIR bands into single raster **** \n\n")

tile_size = 512
stackTileExploded_df = sedona.sql(f'''
   with exp as ( SELECT *, 
        RS_StackTileExplode(Array(rast_red, 
                                  rast_nir
                                  ),
                            0, 
                            {tile_size}, {tile_size},
                            false)
    FROM asset_items
    )
    SELECT
        id,
        x,
        y,
        datetime,
        grid_code,
        tile as rast,
        RS_ENVELOPE(tile) as geometry,
        RS_NumBands(tile) as band_cnt
    FROM 
        exp
''')

stackTileExploded_df = stackTileExploded_df.repartition(1000)

stackTileExploded_df.createOrReplaceTempView("tiled_raster")


# ## Calculating NDVI index

# In[7]:

print("**** Calculating NDVI for House Sales **** \n\n")

ndvi_house = sedona.sql("""

WITH ndvi_raster AS (
SELECT
        a.geometry_buffer,
        a.sale_id,
        ndvi
FROM
    house_sales a
JOIN
    (SELECT
       geometry,
       datetime,
       RS_MapAlgebra(rast, 'D', 'out = (rast[1] > 0 && rast[0] > 0) ? (rast[1] - rast[0]) / (rast[1] + rast[0]) : null;') as ndvi
     FROM tiled_raster
     WHERE NOT isnan(RS_SummaryStats(rast,'max'))) b
ON
   ST_Intersects(b.geometry,ST_TRANSFORM(a.geometry_buffer,"EPSG:4269","EPSG:32610"))
),
zonal_stats (
SELECT
    RS_ZONALSTATS(ndvi,geometry_buffer,'mean') mean_ndvi,
    sale_id
FROM
    ndvi_raster
)

SELECT 
    AVG(mean_ndvi) AS mean_ndvi,
    sale_id
FROM
    zonal_stats
GROUP BY
    sale_id

""")


# In[8]:


ndvi_house.createOrReplaceTempView("ndvi_house")


# In[9]:

# In[23]:


database = "gde_silver"


# In[33]:


sedona.sql(f"CREATE DATABASE IF NOT EXISTS org_catalog.{database}").show()


# In[24]:

print("**** Writing NDVI Enriched House Sales to Silver Table **** \n\n)

sedona.sql("""

SELECT 
    a.*,
    b.mean_ndvi as mean_ndvi_500m_radius
FROM
    house_sales a
JOIN
    ndvi_house b
ON
    a.sale_id = b.sale_id

""").drop("geometry_buffer").writeTo(f"org_catalog.{database}.house_sales_silver").createOrReplace()


# In[ ]:

print("**** NDVI Enrichment Complete! **** \n\n")
