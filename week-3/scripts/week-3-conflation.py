#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *


# In[ ]:


config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'
sedona.sql(f'CREATE DATABASE IF NOT EXISTS org_catalog.{database}')


# In[ ]:


prefix = 's3://wherobots-examples/gdea-course-data/raw-data/'


# In[ ]:


homes = sedona.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('skipRows', 1) \
    .load(f'{prefix}' + 'sales_king_county.csv')


# In[ ]:


homes.writeTo(f"org_catalog.gde_bronze.king_co_homes").createOrReplace()


# In[ ]:


sedona.sql(f'''
alter table org_catalog.gde_bronze.king_co_homes add column geometry geometry
''')


# In[ ]:


sedona.sql(f'''
update org_catalog.gde_bronze.king_co_homes
set geometry = st_point(longitude, latitude) 
''')


# In[ ]:


import wkls

washington = wkls.us.wa.wkt()
seattle = wkls.us.wa.seattle.wkt()
kirkland = wkls.us.wa.kirkland.wkt()
bellevue = wkls.us.wa.bellevue.wkt()


# In[ ]:


sedona.sql(f'''
create or replace table org_catalog.gde_bronze.king_co_homes_conflated as
select a.*,
a.geometry as point,
b.geometry as polygon,
b.id as overture_id,
b.height
from org_catalog.gde_bronze.king_co_homes a
join wherobots_open_data.overture_maps_foundation.buildings_building b
on st_knn(a.geometry, b.geometry, 1, true, 100)
where st_intersects(a.geometry, ST_GeomFromWKT('{kirkland}'))
and st_intersects(b.geometry, ST_GeomFromWKT('{kirkland}'))
''')

