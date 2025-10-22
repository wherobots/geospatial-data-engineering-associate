#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'


# In[ ]:


# Seismic Hazards Table

sedona.sql(f'''
update org_catalog.gde_bronze.king_co_homes
set geometry = st_point(longitude, latitude) 
''')


# In[ ]:


# Seismic Hazards Table

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_seismic_hazards
select                                                                                                      
a.sale_id,                                                                                                  
b.OBJECTID as hazard_id,                                                                                                  
b.HAZARD as hazard                                                                                                     
from org_catalog.gde_bronze.king_co_homes a 
join org_catalog.gde_bronze.seismic_hazards_bronze b  
on ST_Intersects(a.geometry, b.geometry)
''')


# In[ ]:


# Flood Hazards Table

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_flood_hazards
select                                                                                                      
a.sale_id,                                                                                                  
b.FLD_AR_ID as flood_zone_id,                                                                                                  
b.FLD_ZONE as flood_zone                                                                                                     
from org_catalog.gde_bronze.king_co_homes a 
join org_catalog.gde_bronze.fema_flood_zones_bronze b  
on st_contains(b.geometry, a.geometry)
''')


# In[ ]:


# School Achievement Table

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_school_scores
select                                                                                                      
a.sale_id,                                                                                                  
avg(b.high_achievement_percent) as avg_school_high_achievement                                                                                                   
from org_catalog.gde_bronze.king_co_homes a 
join org_catalog.{database}.school_data b  
on st_knn(a.geometry, b.geometry, 5)
group by a.sale_id
''')

