#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sedona.spark import *

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[3]:


database = 'gde_bronze'


# In[6]:


sedona.sql(f'''
update org_catalog.{database}.fema_flood_zones_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:4326')
''')


# In[7]:


sedona.sql(f'''
update org_catalog.{database}.gen_land_use_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:6599', 'EPSG:4326')
''')


# In[11]:


sedona.sql(f'''
update org_catalog.{database}.sherrif_districts_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:2926', 'EPSG:4326')
''')


# In[12]:


sedona.sql(f'''
update org_catalog.{database}.bike_lanes_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:2926', 'EPSG:4326')
''')


# In[ ]:


sedona.sql(f'''
update org_catalog.{database}.fema_nri_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:4326')
''')


# In[19]:


sedona.sql(f'''
update org_catalog.{database}.school_sites_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:2926', 'EPSG:4326')
''')


# In[20]:


sedona.sql(f'''
update org_catalog.{database}.seismic_hazards_bronze
set geometry = ST_Transform(ST_MakeValid(geometry),  'EPSG:2926', 'EPSG:4326')
''')


# In[21]:


sedona.sql(f'''
update org_catalog.{database}.block_groups_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:4326')
''')


# In[22]:


sedona.sql(f'''
update org_catalog.{database}.transit_routes_bronze
set geometry = ST_Transform(ST_MakeValid(geometry),  'EPSG:2926', 'EPSG:4326')
''')


# In[23]:


sedona.sql(f'''
update org_catalog.{database}.transit_stops_bronze
set geometry = ST_Transform(ST_MakeValid(geometry),  'EPSG:2926', 'EPSG:4326')
''')


# In[25]:


sedona.sql(f'''
update org_catalog.{database}.wildfires_bronze
set geometry = ST_Transform(ST_MakeValid(geometry), 'EPSG:4326')
''')


# In[28]:


sedona.sql(f'''
update org_catalog.{database}.schools_bronze
set geometry = ST_SetSRID(ST_MakeValid(geometry), 4326)
''')


# In[ ]:




