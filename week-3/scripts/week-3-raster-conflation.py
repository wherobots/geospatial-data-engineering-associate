#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sedona.spark import *
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException

config = SedonaContext.builder() \
    .getOrCreate()

sedona = SedonaContext.create(config)


# In[4]:


sedona.sql("""

CREATE OR REPLACE TABLE org_catalog.gde_silver.house_sales_silver AS
SELECT
    a.*,
    b.mean_ndvi_500m_radius,
    c.elevation_standard_deviation,
    c.elevation_mean,
    c.elevation_min,
    c.elevation_max,
    d.mean_tri
FROM
    org_catalog.gde_bronze.king_co_homes a
JOIN
    org_catalog.gde_silver.house_sales_ndvi_silver b
ON
    a.sale_id = b.sale_id

JOIN
    org_catalog.gde_silver.house_sales_zonal_silver c
ON
    a.sale_id = c.sale_id

JOIN
    org_catalog.gde_silver.house_sales_tri_silver d
ON
    a.sale_id = d.sale_id


""")


# In[ ]:




