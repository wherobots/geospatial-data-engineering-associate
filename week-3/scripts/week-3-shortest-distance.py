#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sedona.spark import *
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException

config = SedonaContext.builder() \
    .getOrCreate()

sedona = SedonaContext.create(config)


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

# In[3]:


try:
    print(" **** Trying to load house_sales_silver dataset from gde_silver database **** \n\n")
    house_sales_df = (
        sedona.table(f"org_catalog.gde_silver.king_co_homes")
                .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}'))")
                .withColumn("geometry_buffer", expr("ST_Buffer(geometry, 500, true)"))
    )
    print(" **** house_sales_silver dataset found and loaded. **** \n\n")
except AnalysisException as e:
    print(" **** house_sales_silver table doesn't exist, reading the gde_bronze.house_sales_bronze **** \n\n")

    house_sales_df = (
        sedona.table(f"org_catalog.gde_bronze.house_sales_king_co_homesbronze")
                .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}'))")
                .withColumn("geometry_buffer", expr("ST_Buffer(geometry, 500, true)"))
    )


house_sales_df.createOrReplaceTempView("house_sales")


# ## Loading the water bodies dataset

# In[4]:


sq_ft_lake_area = "25000"


# In[5]:

print(" **** Loading water bodies dataset **** \n\n")

water_bodies = (sedona.table("org_catalog.gde_bronze.water_bodies_bronze")
                .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}')) AND SHAPE_Area < {sq_ft_lake_area}")
)

water_bodies.createOrReplaceTempView("water_bodies")


# In[8]:

print(" **** Calculating the closest water body for each house sale **** \n\n")

water_closest_house = sedona.sql(f"""

SELECT 
    h.*, 
    ST_DistanceSphere(h.geometry, w.geometry) AS distance_to_nearest_water_body_m
FROM 
    house_sales h, 
    water_bodies w
WHERE 
    ST_KNN(h.geometry, w.geometry, 1)

""")


# In[ ]:

print(" **** Writing the enriched house sales data to gde_silver.house_sales_silver **** \n\n")

water_closest_house.drop("geometry_buffer").writeTo("org_catalog.gde_silver.king_co_homes").createOrReplace()
