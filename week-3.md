# Instructions

- Each section should be a job
- Assets
    - Notebook - Scaled Down (specific area)
    - Job - complete run
- Create a new "silver" table that contains the values tied to a home ID
- Import home sales

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

## Zonal Stats - Furqaan

- Slope of parcel
- Elevation stats
- Elevation change within N kilometers (ruggedness index)

## NDVI - Furqaan

- Greenspace within N kilometers


# Complete

## ~~Clean Up Tasks - Matt~~

- [x] Join Census demos to polygons
- [x] Clean and drop CSV header rows
- [x] Schools to scores

## ~~Conflation~~

- [x] Done

## ~~Simple Joins - Matt~~

- [x] Seismic Areas - which one is it in
- [x] Flood Plain Indicator
- [x] School Quality

## ~~Area Weighted Interpolation - Matt~~

- [X] Median Income
- [X] Total Population
- [X] Pop Density
- [X] Median Age
- [] Zoning - Running into some issues here with the join/intersection
    - [X] Single Family
    - [X] Multi Family
    - [X] Industrial
    - [X] Retail


## ~~Buffer or Nearest Within - Matt~~

- [x] Distance to nearest park
- [x] Distance to "Downtown"

## ~~Determine Road Arteries + KNN - Matt~~

- [x] Major intersection proximity (ST_Intersection -> POINT)

## 3D Visibility -  Matt

- View of Mt. Rainer + Elevation
- View of Space Needle + Elevation

https://abelvm.github.io/sql/isovists/


- Get buildings and elev at centroids of buildings