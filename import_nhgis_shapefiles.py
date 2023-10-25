import glob, json, os
from io import StringIO
from functools import cache
import numpy as np
import pandas as pd
import geopandas as gpd
from psql_utils.epsql import get_table_name, get_schema, TempSchema

#import importlib, sys
#if mod := sys.modules.get("psql_utils.epsql"): importlib.reload(mod)
import psql_utils.epsql as epsql

#if mod := sys.modules.get("psql_utils.nhgis_api"): importlib.reload(mod)
from psql_utils.nhgis_api import NhgisApi

# Connect to the database

@cache
def engine():
    return epsql.Engine()

@cache
def api():
    return NhgisApi()

nhgis_geo_schema = "nhgis_geo_wgs84"

@cache
def nhgis_geo_table_name(year: int | str, geo_level_id: str, basis_id: str|None = None):
    if basis_id is not None:
        return(f"{nhgis_geo_schema}.{geo_level_id}_{year}_{basis_id}")
    else:
        for table_name in sorted(engine().list_tables(schema=nhgis_geo_schema), reverse = True):
            if table_name.startswith(f"{geo_level_id}_{year}"):
                return f"{nhgis_geo_schema}.{table_name}"
        raise Exception(f"No NHGIS geometry table found for year={year} level={geo_level_id}")

def nhgis_geo_download_year(year: int | str, geo_level_ids: list[str] = ["state", "county", "place", "tract", "blockgroup", "block"]):
    metadata = api().get_shapefiles_metadata().query(f"year == '{year}'")[['geographic_level_id', 'basis_id']].drop_duplicates().to_dict(orient='records') # type: ignore
    for rec in metadata:
        if rec["geographic_level_id"] in geo_level_ids:
            nhgis_geo_download_year_level_basis(year, rec['geographic_level_id'], rec['basis_id'])

# Assume this directory is in our working dir
nhgis_geo_downloads = "nhgis_geo_downloads"

def find_or_download_extracts(year: str|int, geo_level_id: str, basis_id: str, table_name: str) -> list[str]:
    bare_table_name = get_table_name(table_name)
    table_dir = f"{nhgis_geo_downloads}/{bare_table_name}"
    os.makedirs(table_dir, exist_ok=True)
    extract_requests_path = f"{table_dir}/extract_requests.json"
    base_query = f"year == '{year}' and geographic_level_id == '{geo_level_id}' and basis_id == '{basis_id}'"
    # Look first for US-wide shapefile, and use if exists
    shapefiles = api().get_shapefiles_metadata().query(f"{base_query} and extent == 'United States'") # type: ignore
    if len(shapefiles) == 0:
        # Otherwise, use all the shapefiles found
        shapefiles = api().get_shapefiles_metadata().query(base_query) # type: ignore

    shapefile_names: list[str] = shapefiles["name"].to_list() # type: ignore
    if not os.path.exists(extract_requests_path):
        extract_numbers: list[int] = []
        group_size = 5
        for i in range(0, len(shapefile_names), group_size):
            group_names = shapefile_names[i:i+group_size]
            extract_numbers.append(api().request_extract(shapefile_names=group_names))
        open(extract_requests_path, "w").write(json.dumps(extract_numbers) + "\n")
    extract_numbers = json.load(open(extract_requests_path))
    already_done = 0
    already_downloaded = 0
    for extract_number in extract_numbers:
        extract_done_path = f"{table_dir}/extract_{extract_number}_done"
        if os.path.exists(extract_done_path):
            already_done += 1
            continue
        extract_dir = f"{table_dir}/extract_{extract_number}"
        if os.path.exists(extract_dir):
            already_downloaded += 1
        else:
            api().download_extract(extract_number, extract_dir)
    if already_done:
        print(f"{table_name}: {already_done} extracts already complete")
    if already_downloaded:
        print(f"{table_name}: {already_downloaded} extracts already downloaded")

    pattern = f"{table_dir}/extract_*/shapefiles/*"
    shapefile_dirs = sorted(glob.glob(pattern))
    if len(shapefile_dirs) == 0:
        raise Exception(f"No shapefile directories found with pattern {pattern}")
    
    shapefiles: list[str] = []
    for shapefile_dir in shapefile_dirs:
        dir_shapefiles = sorted(glob.glob(f"{shapefile_dir}/*.shp"))
        if len(dir_shapefiles) == 0:
            raise Exception(f"No shapefiles found in {shapefile_dir}")
        dir_shapefile = dir_shapefiles[0]
        if len(dir_shapefiles) > 1:
            if bare_table_name in ["tract_1990_tl2000", "tract_2000_tl2000"]:
                dir_shapefile = dir_shapefiles[0]
            else:
                raise Exception(f"More than one shapefile found in {shapefile_dir}")
        shapefiles.append(dir_shapefile)

    print(f"{table_name}: Found {len(shapefiles)} shapefiles from pattern {pattern}")
    return shapefiles


def shapefile_to_postgis(shapefile: str, table_name: str) -> int:
    bare_table_name = get_table_name(table_name)
    schema = get_schema(table_name)
    gdf = read_nhgis_shapefile_as_wgs84(shapefile) # type: ignore
    with engine().connect() as con:
        gdf.to_postgis(bare_table_name, con, schema=schema, if_exists='append') # type: ignore

    engine().execute(f'CREATE INDEX IF NOT EXISTS {bare_table_name}_geom_idx ON {table_name} USING GIST (geom)')
    #engine().execute(f'CREATE INDEX IF NOT EXISTS {bare_table_name}_geoid_idx ON {table_name} (geoid)')
    return len(gdf) # type: ignore

def nhgis_geo_download_year_level_basis(year: int|str, geo_level_id: str, basis_id: str):
    table_name = nhgis_geo_table_name(year, geo_level_id, basis_id)
    engine().execute(f"CREATE SCHEMA IF NOT EXISTS {get_schema(table_name)}")
    if engine().table_exists(table_name):
        print(f"{table_name}: already exists")
        add_geoid_column_year_level_basis(year, geo_level_id, basis_id)
        return
    
    bare_table_name = get_table_name(table_name)
    schema = get_schema(table_name)
    
    with TempSchema(engine(), prefix=schema) as tmp_schema:
        tmp_table_name = f"{tmp_schema}.{bare_table_name}"
        shapefiles = find_or_download_extracts(year, geo_level_id, basis_id, table_name)
        nrecords = 0

        for shapefile in shapefiles:
            nrecords += shapefile_to_postgis(shapefile, tmp_table_name)
        
        # Move table to final schema
        engine().execute(f"ALTER TABLE {tmp_table_name} SET SCHEMA {schema}")
    
    print(f"{table_name}: Created with {nrecords} records from {len(shapefiles)} shapefile(s)")
    add_geoid_column_year_level_basis(year, geo_level_id, basis_id)

def read_nhgis_shapefile_as_wgs84(filename: str) -> gpd.GeoDataFrame: # type: ignore
    print(f"  Reading {filename} and reprojecting")
    gdf: pd.DataFrame = gpd.read_file(filename) # type: ignore
    epsql.sanitize_column_names(gdf, inplace=True)
    force_dtypes = {'aland10': np.float64, 'awater10': np.float64}
    for col_name, dtype in force_dtypes.items():
        if col_name in gdf.columns and gdf[col_name].dtype != dtype:
            print(f"{os.path.basename(filename)}:  Converting {col_name} from {gdf[col_name].dtype} to {dtype}")
            gdf[col_name] = gdf[col_name].astype(dtype) # type: ignore

    gdf.rename_geometry('geom', inplace=True) # type: ignore
    gdf.to_crs(epsg=4326, inplace = True) # type: ignore
    print(f"  Read {len(gdf)} records from {filename}") # type: ignore
    return gdf # type: ignore

def create_column(table_name: str, col: str, col_type: str, expr: str):
    print(f"{table_name}: Creating {col} from {expr}")
    engine().execute(f"""
        BEGIN;
        ALTER TABLE {table_name} ADD COLUMN {col} {col_type};
        UPDATE {table_name} SET {col} = {expr};
        COMMIT;""")

def column_width_range(table_name: str, col_name: str):
    minmax = engine().execute_returning_dicts(f"SELECT min(length({col_name})), max(length({col_name})) from {table_name}")[0]
    return (minmax['min'], minmax['max'])

def check_and_index_geoid_column(year: int|str, level: str, basis_id: str, create_index: bool = True):
    table_name = nhgis_geo_table_name(year, level, basis_id)
    required_width = {
        "state": (2, 2),
        "county": (5, 5),
        "tract": (11, 11),
        "blockgroup": (12, 12),
        "block": (15, 15),
        "place": (7, 7)
    }[level]
    if year == 1990 and level == "block":
        required_width = (14, 15)
    actual_width = column_width_range(table_name, "geoid")
    if required_width != actual_width:
        raise Exception(f"{table_name}: Geoid column has width range {actual_width} but should be {required_width}")
    # Count number of NULLs in geoid column
    null_count = engine().execute_returning_value(f"SELECT count(*) from {table_name} where geoid is null")
    if null_count > 0:
        raise Exception(f"{table_name}: Geoid column has {null_count} NULLs")
    if create_index:
        # Make geoid column be primary key if not already
        if engine().execute_returning_value(f"""
            SELECT count(*) from information_schema.table_constraints 
            where table_name = '{get_table_name(table_name)}' 
                and table_schema = '{get_schema(table_name)}'
                and constraint_type = 'PRIMARY KEY'""") == 0:
            # Workaround:  NHGIS 2010/TL2020 block data has duplicate records
            if table_name == "nhgis_geo_wgs84.block_2010_tl2020":
                # Ensure only one record per geoid, by deleting duplicates
                print(f"{table_name}: Deleting duplicate records")
                ndel = engine().execute_delete(f"""
                    DELETE FROM {table_name} WHERE ctid NOT IN (
                        SELECT min(ctid) FROM {table_name} GROUP BY geoid
                    );""")
                print(f"{table_name}: Deleted {ndel} records")
            print(f"{table_name}: Making geoid column primary key")
            engine().execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (geoid);")
    
    msg_parts: list[str] = []

    if required_width[1] == 15:
        nblocks = engine().execute_returning_value(f"SELECT count(geoid) from {table_name}")
        msg_parts.append(f"{nblocks} blocks")
        assert nblocks >= 7184329, f"{table_name}: Only {nblocks} blocks found"
    if required_width[1] >= 12:
        nblockgroups = engine().execute_returning_value(f"SELECT count(distinct(left(geoid,12))) from {table_name}")
        msg_parts.append(f"{nblockgroups} block groups")
        assert nblockgroups >= 208672, f"{table_name}: Only {nblockgroups} block groups found"
    if required_width[1] >= 11:
        ntracts = engine().execute_returning_value(f"SELECT count(distinct(left(geoid,11))) from {table_name}")
        msg_parts.append(f"{ntracts} tracts")
        assert ntracts >= 60947, f"{table_name}: Only {ntracts} tracts found"
    if required_width[1] >= 5 and level != "place":
        ncounties = engine().execute_returning_value(f"SELECT count(distinct(left(geoid,5))) from {table_name}")
        msg_parts.append(f"{ncounties} counties")
        assert ncounties >= 3141, f"{table_name}: Only {ncounties} counties found"
    nstates = engine().execute_returning_value(f"SELECT count(distinct(left(geoid,2))) from {table_name}")
    assert nstates >= 51, f"{table_name}: Only {nstates} states found"
    msg_parts.append(f"{nstates} states")

    print(f"{table_name}: {', '.join(msg_parts)}")

def add_geoid_column_year_level_basis(year: int|str, level: str, basis_id: str):
    table_name = nhgis_geo_table_name(year, level, basis_id)
    if engine().table_column_exists(table_name, 'geoid'):
        print(f"{table_name}: Column geoid already exists")
        check_and_index_geoid_column(year, level, basis_id)
        return
    geoid_recipes: pd.DataFrame = (
        pd.read_csv( # type: ignore
            StringIO("\n".join([
                "level      year expr",
                "*          *    geoid00",
                "*          *    geoid10",
                "*          *    geoid20",
                "state      *    statefp00",
                "state      *    statefp",
                "county     *    cntyidfp00",
                "county     *    cntyidfp",
                "county     *    substr(gisjoin,2,2)||substr(gisjoin,5,3)", # G{01}0{003}
                "place      *    substr(gisjoin,2,2)||substr(gisjoin,5,5)", # G{01}0{00100}
                "tract      *    left(state,2)||left(county,3)||tract00",
                "tract      *    left(state,2)||left(county,3)||tract10",
                "tract      *    left(state,2)||le,ft(county,3)||tract20",
                "tract      2000 substr(gisjoin,2,2)||substr(gisjoin,5,3)||substr(gisjoin,9,6)", # G{01}0{003}0{001000}
                "tract      1990 substr(gisjoin,2,2)||substr(gisjoin,5,3)||substr(gisjoin||'00',9,6)", # G{01}0{003}0{0106} ,or G{01}0{003}0{0,10701}
                "blockgroup *    bkgpidfp00",
                "blockgroup *    stfid",
                "block      *    blkidfp00",
                "block      *    stfid",
            ])),
            sep=r'\s+'))

    exprs_to_try: list[str] = geoid_recipes.query( # type: ignore
        f"(level == '{level}' or level == '*') and (year == '{year}' or year == '*')")['expr']
    
    for expr in exprs_to_try:
        try:
            engine().execute(f"SELECT {expr} FROM {table_name} LIMIT 1")
        except:
            continue
        create_column(table_name, "geoid", 'VARCHAR', expr)
        check_and_index_geoid_column(year, level, basis_id)
        return
    print(f"Could not create geoid column for {table_name} after trying {exprs_to_try}")
    # Display some rows if we're executing in a notebook
    try:
        from IPython.display import display # type: ignore
        display(engine().execute_returning_df(f"SELECT * FROM {table_name} LIMIT 20;"))
    except:
        pass

