Phase 5:
DONE Commits
Are columns actually integers, or strings?

Phase 4:
Get Oakland geometry
Intersect Oakland for blocks
Use Census API to get 2020, 2010, 2000 populations
Use NHGIS API to get 1990 populations





Phase 3:
DONE Plume geoms for Mickey

Phase 2:
DONE Intersection utility

Phase 1:
DONE * Create semaphore to limit simultaneous downloads
DONE * Create temporary schema
DONE * Write initially to temporary schema
DONE * Write each gdf to database and reindex
DONE * Move from temporary schema
DONE * Remove temporary schema
DONE * Construct geoids
DONE * Move .ipynb to .py
* View some blocks and tracts


Backlog:
Consider group(TABLENAME) syntax, e.g.
https://api.census.gov/data/2000/dec/sf1?get=group(H001)&for=state















NHGIS geoids

state_1990_tl2000, state_2000_tl2000
                        gisjoin[1:3]
                        gisjoin2 shifted by 1

state_2000_tl2010
                        statefp00


county_1990_tl2000, county_1990_tl2008
                        state[:2] + county[:3]
                        nhgisst[:2] + nhgiscty[:3]
                        gisjoin[1:3] + [4:7] (len=7)
                        gisjoin2 shifted by 1

place: punt

tract_1990_tl2000: punt

tract_1990_tl2008
                        state + county + tract

blockgroup_1990_tl2000, block_1990_tl2000
                        stfid


