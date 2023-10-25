Phase 7:
Understand the missing-data datblase in ACS2021 and improve code to not give exceptions
Create ACS2022 5-year (When avail 12/7/23)
Create Decennial 2020 DHC table
Create Decennial 2020 other tables

Test counts for each table, compare against geometry

Is the following still needed?
    Delete 2010 and 2000, and try to recreate, watching for geoid conflicts    
        Warning if geoids are outside expected range
        Warning if record count doesn’t match expected

Consider adding to requirements.txt

DONE Phase 6:

DONE Download ACS2021 5-year except for "missing data" tables

DONE Don't show progress bars for already-done things
DONE Add per-dataset progress bar in highlight color
DONE Make int nullable for tables that have nulls in dec2010

DONE Phase 5:
DONE Commits
DONE Convert strings to ints for decennial census

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


