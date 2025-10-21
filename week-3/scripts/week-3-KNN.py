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

# seattle_dt = 'POLYGON ((-122.354307 47.612065, -122.318087 47.612065, -122.318087 47.626645, -122.354307 47.626645, -122.354307 47.612065))'

washington = wkls.us.wa.wkt()
seattle = wkls.us.wa.seattle.wkt()
kirkland = wkls.us.wa.kirkland.wkt()
bellevue = wkls.us.wa.bellevue.wkt()


# In[ ]:


# SedonaKepler.create_map(sedona.sql(f"SELECT ST_GeomFromWKT('{kirkland}') AS geometry"), name="Area of Interest")


# ## Grab residential Buildings
# 
# The Spatial Filter gets pushed down to storage level to only grab residential buildings that intersect the AOI

# In[ ]:


buildings_df = sedona.table("wherobots_open_data.overture_maps_foundation.buildings_building")\
                .filter(f"""
                    ST_Intersects(geometry, ST_GeomFromWKT('{kirkland}'))
                    AND subtype == 'residential'
                """)
buildings_df.createOrReplaceTempView("buildings")
# buildings_df.show(5)
# buildings_df.count()


# ## Grab Overture Places
# 
# Same Spatial filter pushdown concept with Places of Interest (POI) dataset.
# 
# Here, we apply a buffer of 1000m to the AOI to make sure buildings near the edge of the AOI have coverage of POIs on all sides

# In[ ]:


places_df = sedona.table("wherobots_open_data.overture_maps_foundation.places_place")\
                .filter(f"""
                    ST_Intersects(
                        geometry, 
                        ST_Buffer(ST_GeomFromWKT('{kirkland}'), 1000, true)
                    )
                """)
places_df.createOrReplaceTempView("places")
# places_df.show(5)
# places_df.count()


# ### Coffee Shops

# In[ ]:


coffee_df = places_df.filter("categories.primary == 'coffee_shop'")
coffee_df.createOrReplaceTempView("coffee")
# coffee_df.show(3)
# coffee_df.count()


# ### Grocery Stores

# In[ ]:


grocery_df = places_df.filter("categories.primary == 'grocery_store'")
grocery_df.createOrReplaceTempView("grocery")
# grocery_df.show(3)
# grocery_df.count()


# ### Child Care

# In[ ]:


child_care_df = places_df.filter("categories.primary == 'child_care_and_day_care'")
child_care_df.createOrReplaceTempView("child_care")
# child_care_df.show(3)
# child_care_df.count()


# ### Gyms

# In[ ]:


gym_df = places_df.filter("categories.primary == 'gym'")
gym_df.createOrReplaceTempView("gym")
# gym_df.show(3)
# gym_df.count()


# ### Hospitals

# In[ ]:


hospital_df = places_df.filter("categories.primary == 'hospital'")
hospital_df.createOrReplaceTempView("hospital")
# hospital_df.show(3)
# hospital_df.count()


# ### Transit Stops

# In[ ]:


transit_df = places_df.filter("categories.primary == 'transportation'")
transit_df.createOrReplaceTempView("transit")
# transit_df.show(3)
# transit_df.count()


# ### Schools

# In[ ]:


schools_df = sedona.table("org_catalog.gde_bronze.schools_bronze")\
                .filter(f"ST_Intersects(geometry, ST_GeomFromWKT('{bellevue}'))")
schools_df.createOrReplaceTempView("school")
# schools_df.show(3)
# schools_df.count()


# ## Perform KNN join between all essential amenities and residential buildings in downtown Seattle area

# In[ ]:

nearest_places_df = sedona.sql('''
    SELECT
        buildings.*,

        -- Nearest coffee shop
        coffee.id AS coffee_id,
        coffee.names['primary'] AS coffee_name,
        coffee.geometry AS coffee_geometry,
        ST_DistanceSpheroid(buildings.geometry, coffee.geometry) AS coffee_distance,

        -- Nearest grocery store
        grocery.id AS grocery_id,
        grocery.names['primary'] AS grocery_name,
        grocery.geometry AS grocery_geometry,
        ST_DistanceSpheroid(buildings.geometry, grocery.geometry) AS grocery_distance,

        -- Nearest child care
        child_care.id AS child_care_id,
        child_care.names['primary'] AS child_care_name,
        child_care.geometry AS child_care_geometry,
        ST_DistanceSpheroid(buildings.geometry, child_care.geometry) AS child_care_distance,

        -- Nearest gym
        gym.id AS gym_id,
        gym.names['primary'] AS gym_name,
        gym.geometry AS gym_geometry,
        ST_DistanceSpheroid(buildings.geometry, gym.geometry) AS gym_distance,

        -- Nearest hospital
        hospital.id AS hospital_id,
        hospital.names['primary'] AS hospital_name,
        hospital.geometry AS hospital_geometry,
        ST_DistanceSpheroid(buildings.geometry, hospital.geometry) AS hospital_distance,

        -- Nearest school
        school.SchoolCode AS school_code,
        school.School AS school_name,
        school.SchoolCategory AS school_category,
        school.geometry AS school_geometry,
        ST_DistanceSpheroid(buildings.geometry, school.geometry) AS school_distance,

        -- Nearest transit stop
        transit.id AS transit_id,
        transit.names['primary'] AS transit_name,
        transit.geometry AS transit_geometry,
        ST_DistanceSpheroid(buildings.geometry, transit.geometry) AS transit_distance

    FROM buildings

    -- Join for coffee shops
    JOIN coffee
    ON ST_KNN(buildings.geometry, coffee.GEOMETRY, 1, true)
    
    -- Join for grocery shops
    JOIN grocery
    ON ST_KNN(buildings.geometry, grocery.GEOMETRY, 1, true)

    -- Join for child care
    JOIN child_care
    ON ST_KNN(buildings.geometry, child_care.GEOMETRY, 1, true)

    -- Join for gym
    JOIN gym
    ON ST_KNN(buildings.geometry, gym.GEOMETRY, 1, true)

    -- Join for hospital
    JOIN hospital
    ON ST_KNN(buildings.geometry, hospital.GEOMETRY, 1, true)

    -- Join for school
    JOIN school
    ON ST_KNN(buildings.geometry, school.GEOMETRY, 1, true)

    -- Join for transit
    JOIN transit
    ON ST_KNN(buildings.geometry, transit.GEOMETRY, 1, true)
''')

# nearest_places_df.cache().count()


# In[ ]:


# nearest_places_df.show(5)


# In[ ]:


# Create a new Havasu (Iceberg) database

database = 'gde_silver'

sedona.sql(f'CREATE DATABASE IF NOT EXISTS org_catalog.{database}')
database


# In[ ]:


nearest_places_df.writeTo(f"org_catalog.{database}.residential_buildings_amenites_silver").createOrReplace()


# In[ ]:




