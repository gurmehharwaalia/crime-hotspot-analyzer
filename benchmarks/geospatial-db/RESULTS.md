# Geospatial Database Benchmark Results

## Databases Tested
- PostGIS (PostgreSQL + PostGIS extension)
- MongoDB (local instance, same geospatial engine as Atlas)

## Dataset
- 20,000 random points near Seattle/Bellevue
- Spatial indexes enabled in both databases

## Queries Tested
- Radius query (1km)
- Polygon containment query

## Results (10 runs, averaged)
- PostGIS radius (ST_DWithin): 47.00 ms
- PostGIS polygon (ST_Contains): 69.90 ms
- MongoDB radius ($nearSphere): 18.30 ms
- MongoDB polygon ($geoWithin): 23.59 ms

## Conclusion
MongoDB demonstrated lower average query times for both radius and polygon queries in this benchmark and was selected as the primary geospatial database for the project.

