#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sedona.spark import *
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException

config = SedonaContext.builder() \
    .getOrCreate()

sedona = SedonaContext.create(config)


# # Recommended runtime
# 
# ## Tiny
# 
# # Cost: 
# 
# Cost of executing this over King County.
# 
# - **>$1.5**
# 
# Cost of executing this over Kirkland.
# 
# - **>$0.4**

# Make sure to filter out the unnecessary rasters outside of King County.
# 
# 
# ```python
# import wkls
# 
# _aoi = wkls.us.wa.kingcounty.wkt()
# ```

# In[ ]:


import wkls


_aoi = wkls.us.wa.kirkland.wkt()


# In[ ]:


house_sales_df = (
    sedona.table(f"org_catalog.gde_bronze.king_co_homes")
            .where(f"ST_Intersects(geometry, ST_GeomFromWKT('{_aoi}'))")
            .withColumn("geometry_buffer", expr("ST_Buffer(geometry, 500, true)"))
)


house_sales_df.createOrReplaceTempView("house_sales")

# In[ ]:


print("**** Loading in elevation raster and filtering by the AOI **** \n\n")

elevation_raster = (
    sedona.table("org_catalog.gde_bronze.elevation_bronze")
        .where(f"RS_Intersects(rast, ST_GeomFromWKT('{_aoi}'))")
)

elevation_raster.createOrReplaceTempView("elevation_raster")


# # Calculate zonal stats on the elevation raster over the AOI
# 
# The AOIs are the house buffered polygon.

# In[ ]:

print("**** Calculating zonal stats for elevation raster over house buffered polygons **** \n\n")

zonal_stats = sedona.sql(f"""

WITH zonal AS (
SELECT 
    RS_ZonalStatsAll(rast, geometry_buffer) AS zonal_stats,
    h.*
FROM
    house_sales h
JOIN
    elevation_raster e
ON
    RS_Intersects(h.geometry_buffer, e.rast)
)

SELECT
    *,
    zonal_stats.min as elevation_min,
    zonal_stats.max as elevation_max,
    zonal_stats.mean as elevation_mean,
    zonal_stats.stddev as elevation_standard_deviation
FROM
    zonal

""")


# In[ ]:

print("**** Writing out the zonal stats enriched house sales to gde_silver.house_sales_zonal_silver **** \n\n")

(zonal_stats.select("sale_id", "elevation_min", "elevation_max", "elevation_mean", "elevation_standard_deviation")
        .writeTo("org_catalog.gde_silver.house_sales_zonal_silver")
        .createOrReplace()
)

# # TRI enrichment
#
# Using the same bronze `king_co_homes` table's dataframe `house_sales_df`.
#
# ## Clipping the elevation raster to AOI
#
# Over here the AOI is the house buffered polygons.

# In[ ]:

print("**** Clipping elevation raster to house buffered polygons **** \n\n")

elevation_raster_clipped = sedona.sql(f"""

SELECT 
    h.*,
    RS_Clip(
        rast,
        1,
        geometry_buffer
    ) AS clipped_rast
FROM
    elevation_raster e
JOIN
    house_sales h
ON
    RS_Intersects(e.rast, h.geometry_buffer)

""")

elevation_raster_clipped.createOrReplaceTempView("elevation_raster_clipped")


# ## Compute TRI rasters with clipped elevation rasters
# 
# TRI rasters are generated using a kernel size of 3x3. The TRI is defined as:
# 
# 
# ![image.png](https://raw.githubusercontent.com/wherobots/geospatial-data-engineering-associate/refs/heads/week-3/week-3/assets/tri_formula.png)
# ![image.png](https://raw.githubusercontent.com/wherobots/geospatial-data-engineering-associate/refs/heads/week-3/week-3/assets/tri_cal_representation.png)
# 
# 
# where xi refers to each of the eight neighbors of the center cell E.

# In[ ]:

print("**** Computing TRI rasters from clipped elevation rasters **** \n\n")

tri_raster = sedona.sql('''
    SELECT
        *,
        RS_MapAlgebra(
            clipped_rast,
            'D',
            '
            if (x() > 0 && x() < width() - 1 && y() > 0 && y() < height() - 1) {
                // Calculate the TRI for each cell that has valid neighbors
                out = (abs(rast - rast[-1, 0]) + abs(rast - rast[1, 0]) + 
                       abs(rast - rast[0, -1]) + abs(rast - rast[0, 1]) + 
                       abs(rast - rast[-1, -1]) + abs(rast - rast[-1, 1]) + 
                       abs(rast - rast[1, -1]) + abs(rast - rast[1, 1])) / 8;
            } else {
                out = 0;  // Set a default or "no data" value for edge cells
            }
            '
        ) AS tri_raster
    FROM elevation_raster_clipped
''')

tri_raster.createOrReplaceTempView("tri_raster")


# In[ ]:

print("**** Calculating TRI zonal stats over house buffered polygons **** \n\n")

tri_enriched = sedona.sql("""

SELECT 
    *,
    RS_SummaryStats(tri_raster, 'mean') as mean_tri

FROM tri_raster

""").select("sale_id", "mean_tri")


# In[ ]:

print("**** Writing out the TRI enriched house sales to gde_silver.house_sales_silver **** \n\n")

tri_enriched.writeTo("org_catalog.gde_silver.house_sales_tri_silver").createOrReplace()


# In[ ]:

print("**** Finished TRI enrichment and writing to gde_silver.house_sales_silver **** \n\n")


