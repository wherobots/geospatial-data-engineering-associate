#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'


# In[ ]:


import wkls

washington = wkls.us.wa.wkt()
seattle = wkls.us.wa.seattle.wkt()
kirkland = wkls.us.wa.kirkland.wkt()
bellevue = wkls.us.wa.bellevue.wkt()


# In[ ]:


# Home Area Buffers

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_buffers as
select                                                                                                      
sale_id,                                                                                                  
ST_Buffer(geometry, 1600, true) as buffer_1mile
from org_catalog.gde_bronze.king_co_homes 
''')


# In[ ]:


# Flood Hazards Table

sedona.sql(f'''
create or replace table org_catalog.{database}.homes_demographics
with overlaps as (
select                                                                                                      
a.sale_id,   
b.total_pop,
b.median_age,
b.median_income,
st_area(
    st_intersection(a.buffer_1mile, b.geometry)
) / st_area(b.geometry) as share                                                                                             
from org_catalog.{database}.homes_buffers a 
join org_catalog.{database}.census_data b  
on st_intersects(b.geometry, a.buffer_1mile)
),
weighted AS (
  SELECT
    sale_id,
    (total_pop * share) AS pop_in_overlap,  
    median_age,
    median_income
  FROM overlaps
)
SELECT
  sale_id,
  SUM(pop_in_overlap) AS est_total_pop,
  SUM(median_income * pop_in_overlap) / NULLIF(SUM(pop_in_overlap), 0) AS pw_median_income_proxy,
  SUM(median_age * pop_in_overlap) / NULLIF(SUM(pop_in_overlap), 0) AS pw_median_age_proxy
FROM weighted
GROUP BY sale_id
''')


# In[ ]:


# Zoning Percentages

sedona.sql(f'''
create or replace table org_catalog.{database}.zoning_overlaps as
select                                                                                                      
a.sale_id,   
b.MASTER_CAT,
b.SUB_CAT,
st_area(
    st_intersection(a.buffer_1mile, b.geometry)
) / st_area(b.geometry) as share                                                                                             
from org_catalog.{database}.homes_buffers a 
join  (select /*+ REPARTITION(800) */ * from org_catalog.gde_bronze.gen_land_use_bronze) b  
on st_intersects(b.geometry, a.buffer_1mile)
where st_isvalid(geometry) is true
and st_intersects(b.geometry, ST_GeomFromText('{kirkland}'))
-- and b.MASTER_CAT not in ('PROW', 'ROW', 'Undesignated')
''')


# In[ ]:


sedona.sql(f'''
create or replace table org_catalog.{database}.homes_zoning_overlaps as
SELECT
  sale_id,
  SUM(CASE WHEN master_cat = 'Industrial' THEN share ELSE 0.0 END) AS industrial,
  SUM(CASE WHEN sub_cat = 'Residential (12+ Units/Acre)' THEN share ELSE 0.0 END) AS urban_residential,
  SUM(CASE WHEN sub_cat = 'Mixed Use' THEN share ELSE 0.0 END) AS mixed_use,
  SUM(CASE WHEN sub_cat = 'Commercial/Office' THEN share ELSE 0.0 END) AS commercial,
  SUM(CASE WHEN master_cat = 'Active Open Space and Recreation' THEN share ELSE 0.0 END) AS recreation,
  SUM(CASE WHEN master_cat = 'Urban Character Residential' THEN share ELSE 0.0 END) AS light_urban,
  SUM(CASE WHEN master_cat = 'Rural Character Residential' THEN share ELSE 0.0 END) AS rural
FROM org_catalog.{database}.zoning_overlaps
GROUP BY sale_id
''')

