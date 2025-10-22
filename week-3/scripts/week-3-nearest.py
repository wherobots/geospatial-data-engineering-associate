#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'


# In[ ]:


# Home Area Buffers

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_distance_to_seattle as
select                                                                                                      
sale_id,                                                                                                  
ST_DistanceSpheroid(geometry, ST_Point(-122.336111, 47.608056)) as distance
from org_catalog.gde_bronze.king_co_homes 
''')


# In[ ]:


# Flood Hazards Table

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_distance_to_park as
select                                                                                                      
a.sale_id,                                                                                                  
ST_DistanceSpheroid(a.geometry, b.geometry) as distance
from org_catalog.gde_bronze.king_co_homes a
join wherobots_open_data.overture_maps_foundation.base_land_use b
on st_knn(a.geometry, b.geometry, 1, true)
where b.subtype in ('golf', 'park', 'pedestrian', 'recreation', 'winter_sports')
and st_intersects(b.geometry, ST_GeomFromText('POLYGON((-122.543146 47.780328, -121.065638 47.780328, -121.065638 47.08409, -122.543146 47.08409, -122.543146 47.780328))'))
and st_areaspheroid(b.geometry) > 5000
''')

