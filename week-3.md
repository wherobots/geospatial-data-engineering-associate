# Instructions

- Each section should be a job
- Assets
    - Notebook - Scaled Down (specific area)
    - Job - complete run
- Create a new "silver" table that contains the values tied to a home ID
- Import home sales

## Conflation

- [x] Done

## Simple Joins - Matt

- [x] Seismic Areas - which one is it in
- [x] Flood Plain Indicator
- [x] School Quality

## KNN - Pranav

- Historic price trend of N=100
- POI (for Walk Scores)
    - Coffee Shop
    - Grocery Store
    - Child Care
    - Gym
    - Hospital
- Bus Stops
- Transit "Stations" or Hubs
- Light Rail Access (within reasonable distance)

## Shortest Path - Furqaan

- Distance to nearest larger water body smaller than X sq meters

## Isochones - Pranav

- Number of "interesting places" within N minutes (drive)

## Area Weighted Interpolation - Matt

- Median Income
- Total Population
- Pop Density
- Median Age
- Zoning 
    - Single Family
    - Multi Family
    - Industrial
    - Retail

## Zonal Stats - Furqaan

- Slope of parcel
- Elevation stats
- Elevation change within N kilometers (ruggedness index)

## Buffer or Nearest Within - Matt

- Distance to nearest park
- Distance to "Downtown"
- Crimes Severe
- Crimes non-Severe

## Determine Road Arteries + KNN - Matt

- Major intersection proximity (ST_Intersection -> POINT)

## NDVI - Furqaan

- Greenspace within N kilometers

## 3D Visibility -  Matt

- View of Mt. Rainer + Elevation
- View of Space Needle + Elevation

https://abelvm.github.io/sql/isovists/

## Simple Calculation - Matt

- Lot Size

# Clean Up Tasks - Matt

- [x] Join Census demos to polygons
- [x] Clean and drop CSV header rows
- Schools to scores

# Gold Table Tasks

- Walk Score


~/Users/mattforrest/Documents/geospatial-data-engineering-associate/week-3/

update org_catalog.gde_bronze.king_co_homes
set geometry = st_point(longitude, latitude) 

select                                                                                                      
a.sale_id,                                                                                                  
b.OBJECTID as  
hazard_id,                                                                                                  
b.HAZARD as 
hazard                                                                                                     
from org_catalog.gde_bronze.king_co_homes a 
join org_catalog.gde_bronze.seismic_hazards_bronze b  
on st_contains(a.geometry, b.geometry)