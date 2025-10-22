#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'


# In[ ]:


# Major Road Intersection

sedona.sql(f'''
create or replace table org_catalog.{database}.roads_proximity as
with roads as (
select *
from wherobots_open_data.overture_maps_foundation.transportation_segment
where st_intersects(geometry, st_geomfromtext('POLYGON((-122.543146 47.780328, -121.065638 47.780328, -121.065638 47.08409, -122.543146 47.08409, -122.543146 47.780328))'))
and class in ('motorway', 'primary', 'secondary', 'tertiary')
),
points as (
SELECT
a.id  AS ida,
b.id  AS idb,
ST_Intersection(a.geometry, b.geometry) AS geom
FROM roads a
JOIN roads b
where st_crosses(a.geometry, b.geometry))

select 
a.sale_id,
st_distancespheroid(a.geometry, points.geom) as distance
from
org_catalog.gde_bronze.king_co_homes a
join points
on st_knn(a.geometry, points.geom, 1, true)
''')

