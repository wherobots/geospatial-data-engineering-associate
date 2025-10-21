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


# ## Grab Area of Interest

# In[ ]:


import wkls

seattle_dt = 'POLYGON ((-122.354307 47.612065, -122.318087 47.612065, -122.318087 47.626645, -122.354307 47.626645, -122.354307 47.612065))'

washington = wkls.us.wa.wkt()
seattle = wkls.us.wa.seattle.wkt()
kirkland = wkls.us.wa.kirkland.wkt()
bellevue = wkls.us.wa.bellevue.wkt()


# ## Grab residential Buildings
# 
# The Spatial Filter gets pushed down to storage level to only grab residential buildings that intersect the AOI

# In[ ]:


buildings_df = sedona.table("wherobots_open_data.overture_maps_foundation.buildings_building")\
                .filter(f"""
                    ST_Intersects(geometry, ST_GeomFromWKT('{seattle}'))
                    AND subtype == 'residential'
                """)
buildings_df.createOrReplaceTempView("buildings")
# buildings_df.show(3)
# buildings_df.count()


# ## Grab Overture Places with pregenerated Drive time Isochrones
# 
# Same Spatial filter pushdown concept with Places of Interest (POI) dataset.
# 
# Here, we apply a buffer of 20KM to the AOI to make sure buildings near the edge of the AOI have coverage of POIs on all sides

# In[ ]:


poi_isochones_df = sedona.table("wherobots_pro_data.overture_maps_foundation.overture_places_with_isochrones")\
                .filter(f"""
                    ST_Intersects(
                        geometry, 
                        ST_Buffer(ST_GeomFromWKT('{seattle}'), 20000, true)
                    )
                """)
poi_isochones_df.createOrReplaceTempView("poi_iso")
# poi_isochones_df.show(3)
# poi_isochones_df.count()


# In[ ]:


history_museums_df = poi_isochones_df.filter("categories.primary ILIKE '%history_museum%'")
history_museums_df.createOrReplaceTempView("museum")
# history_museums_df.show(3)
# history_museums_df.count()


# ## Enriching Residential Buildings with 5-Minute Museum Accessibility via car

# In[ ]:


# get_ipython().run_line_magic('time', '')

res = sedona.sql("""
    SELECT
      b.id,
      FIRST(b.geometry) AS geometry,
      FIRST(b.subtype) AS subtype,
      FIRST(b.class) AS class,
      FIRST(b.names.primary) AS name,
      (SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) > 0) AS has_museum_5min
    FROM buildings b
    LEFT JOIN museum m
      ON ST_Intersects(b.geometry, m.isochrone_5min)
    GROUP BY b.id
""")

# res.cache().count()


# In[ ]:


# res.show(5)


# In[ ]:


# Create a new Havasu (Iceberg) database

database = 'gde_silver'

sedona.sql(f'CREATE DATABASE IF NOT EXISTS org_catalog.{database}')
# database


# In[ ]:


res.writeTo(f"org_catalog.{database}.residential_buildings_near_museum_silver").createOrReplace()


# In[ ]:




