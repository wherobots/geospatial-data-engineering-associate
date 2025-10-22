#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)


# In[ ]:


database = 'gde_silver'


# In[ ]:


# Join Census Data to Polygons

sedona.sql(f'''
create or replace table org_catalog.{database}.census_data as
select
a.geometry,
a.GEOID as geoid,
b._c2 as total_pop, 
c._c2 as median_age,
d._c2 as median_income
from org_catalog.gde_bronze.block_groups_bronze a
join org_catalog.gde_bronze.total_pop_bronze b
on right(b._c0, 12) = a.GEOID
join org_catalog.gde_bronze.median_age_bronze c
on right(c._c0, 12) = a.GEOID
join org_catalog.gde_bronze.median_income_bronze d
on right(d._c0, 12) = a.GEOID
''')


# In[ ]:


# Join School Data to School Points

sedona.sql(f'''
create or replace table org_catalog.{database}.school_data as

with one as (
select 
_c3 as ESDName, 
_c5 as DistrictCode,
_c8 as SchoolCode,
_c9 as SchoolName,
sum(_c18) as total_students,
sum(_c21) as high
from org_catalog.gde_bronze.report_card_bronze
where _c13 = 'All Students'
and _c14 = 'All Grades'
group by 1, 2, 3, 4
)

select
a.geometry,
a.ESDName,
a.School,
b.SchoolCode,
b.SchoolName,
b.high/b.total_students as high_achievement_percent
from org_catalog.gde_bronze.schools_bronze a
join one b
on a.ESDName = b.ESDName
and a.School = b.SchoolName
''')

