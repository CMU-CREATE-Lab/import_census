import json, os, re, requests, sys, threading, time
from collections import defaultdict
from functools import cache
import pandas as pd
from utils.utils import SimpleThreadPoolExecutor, PrCall, ThCall
import numpy as np
from psql_utils.epsql import get_schema, get_table_name, sanitize_table_name, sanitize_column_names
from psql_utils import epsql
from tqdm.notebook import tqdm
from typing import Dict
import sqlalchemy.exc
import psycopg2.errors

# Table metadata
#    Which states are included
#    Which geographic levels are included

def table_coverage_table():
    name = "census.m_table_coverage"
    engine().execute(f"""
    CREATE TABLE IF NOT EXISTS {name} (
        dataset_name text,
        table_name text,
        states jsonb, -- array of state fips codes
        geo_levels jsonb, -- array of geom levels
        PRIMARY KEY (dataset_name, table_name)
    )
    """)
    return name

engine_dict = {}
def engine() -> epsql.Engine: 
    id = (os.getpid(), threading.get_ident())
    if id not in engine_dict:
        engine_dict[id] = epsql.Engine(verbose = False)
    return engine_dict[id]

class NoDataException(Exception):
    pass

class HierarchyException(Exception):
    pass

def census_api_get(base_url, payload):
    payload = payload.copy() # Don't modify the original
    payload['key'] = open("secrets/census_api_key.txt").read().strip()
    retries = 5
    for retry in range(retries):
        if retry:
            print(f"Retry {retry+1} of {retries} for GET {base_url} {payload}")
        response = None
        try:
            response = requests.get(base_url, params=payload)
            # If it looks like a retryable server issue, make exception now
            if response.status_code // 100 not in (2, 4):
                response.raise_for_status()
        except Exception as e:
            print(f"During try {retry+1} of {retries} for GET {response and response.url}, received exception {e}")
            if retry == retries - 1:
                print("Aborting since this is the last retry")
                raise
            continue
        if response.status_code == 200:
            if retry:
                print(f"On retry {retry+1}, successful GET {response.url}")
            return pd.DataFrame(response.json()[1:], columns=response.json()[0])
        if response.status_code == 400 and re.search(r"(unknown|unsupported).*h(ie|ei)rarchy", response.text, re.I):
            raise HierarchyException()
        if response.status_code // 100 == 4:
            # 4xx errors are client errors, so don't retry
            print(f"During try {retry+1} of {retries} for GET {response.url}, aborting due to client error status code {response.status_code} {response.text}")
            response.raise_for_status()
        
        # Otherwise, retry
        time.sleep(5)
    raise Exception("Should never get here")

def get_census_metadata_table():
    census_metadata_table = "census.metadata"
    if not engine().table_exists(census_metadata_table):
        engine.execute(f"""
        CREATE TABLE {census_metadata_table} (
            dataset_name text,
            table_name text,
            variable_name text,
            variable_label text,
            variable_type text,
            variable_group text,
            variable_attributes text,
            variable_values text,
            PRIMARY KEY (dataset_name, table_name, variable_name)
        )
        """)

class CensusApiDataset():
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.schema_name = "census"
        unsorted_vars = requests.get(f'https://api.census.gov/data/{dataset_name}/variables.json').json()['variables']
        patches = {
            '2000/dec/sf1': {
                'P001001': {'predicateType': 'int'},
                'P004001': {'predicateType': 'int'}
            },
            '2000/dec/sf2': {
                'HCT004001': {'predicateType': 'int'}
            },
            '2000/dec/sf3': {
                'P001001': {'predicateType': 'int'}
            },
            '2010/dec/sf1': {
                'P001001': {'predicateType': 'int'},
            }
        }

        for col, patch in patches.get(dataset_name, {}).items():
            for key, value in patch.items():
                print(f"Patching {dataset_name}[{repr(col)}][{repr(key)}]={repr(value)}")
                unsorted_vars[col][key] = value
                
        # if dataset_name in ["2000/dec/sf1", "2010/dec/sf1"] and 'predicateType' not in unsorted_vars['P001001']:
        #     print(f"Patching predicateType='int' for {dataset_name}.P001001")
        #     unsorted_vars['P001001']['predicateType'] = "int"
        self.variables = dict(sorted(unsorted_vars.items()))
        tables_build = defaultdict(dict)
        for var_name, var_info in self.variables.items():
            for table_name in set(var_info['group'].split(',')) - {'N/A'}:
                tables_build[table_name][var_name]=var_info

        self.tables: Dict[str, dict] = dict(sorted(tables_build.items()))
        # self.table_geo_levels = defaultdict(list)
        # for table_name in self.tables.keys():
        #     self.table_geo_levels[table_name].append('state')
        #     self.table_geo_levels[table_name].append('county')
        #     if table_name.startswith('PCO') or table_name.startswith('HCO'):
        #         continue
        #     self.table_geo_levels[table_name].append('tract')
        #     if table_name.startswith('PCT') or table_name.startswith('HCT'):
        #         continue
        #     self.table_geo_levels[table_name].append('blockgroup')
        #     # 2000 decennial sf3 tables are sampled and do not have block data
        #     if dataset_name == "2000/dec/sf3":
        #         continue
        #     # ACS does not have block data
        #     if dataset_name.split('/')[1] == 'acs':
        #         continue
        #     self.table_geo_levels[table_name].append('block')

        self.bad_cols = {}
        #print(f"{dataset_name}: found tables ({', '.join(self.tables.keys())})")
        print(f"{dataset_name}: found {len(self.tables)} tables")

    # Read or create table geography coverage record, in the table_coverage_table()
    @cache
    def get_table_coverage(self, table_name: str):
        def lookup_table_coverage():
            records = engine().execute_returning_dicts(
                f"SELECT states, geo_levels FROM {table_coverage_table()} WHERE dataset_name = '{self.dataset_name}' AND table_name = '{table_name}'")
            assert len(records) <= 1
            if len(records) == 1:
                return records[0]
            else:
                return None
        
        if ret := lookup_table_coverage():
            return ret
        rec = self.get_table_coverage_uncached(table_name)
        sql_rec = {
            "dataset_name": self.dataset_name,
            "table_name": table_name,
            "states": json.dumps(rec["states"]),
            "geo_levels": json.dumps(rec["geo_levels"])
        }
        engine().upsert(table_coverage_table(), ["dataset_name", "table_name"], sql_rec)
        ret = lookup_table_coverage()
        assert ret
        return ret
    
    def get_table_coverage_uncached(self, table_name: str):
        # Is "us" level covered?
        get_args = { 'get': f"group({table_name})", 'for': 'us:*' }



        # Which states are covered?

        get_args = { 'get': f"group({table_name})", 'for': 'county:*' }
        counties = self.api_get(get_args)
        variables = list(self.tables[table_name].keys())
        county_has_data = ~counties[variables].isna().any(axis=1)
        states = sorted(counties[county_has_data]['state'].unique())

        # get_args = { 'get': f"group({table_name})", 'for': 'state:*' }
        # data = self.api_get(get_args)
        # first_var = list(self.tables[table_name].keys())[0]
        # states = sorted(data[~data[first_var].isnull()]["state"])
        if len(states) == 0:
            raise NoDataException(
                f"No states found for {self.dataset_name}.{table_name}\n"
                f"when calling self.api_get({repr(get_args)})")
        
        first_state = states[0]
        geo_levels = ["state"]
        in_ = f"state:{first_state}"
        for geo_level in ["county", "tract", "block group", "block"]:
            try:
                data = self.api_get({ 'get': f"group({table_name})", 'for': geo_level, 'in': in_ })
            except HierarchyException:
                # This level is not available;  assume we've reached the end of the hierarchy
                break
            #if data[first_var].isnull().all():
            if data[variables].isnull().all().all():
                # Geography level is missing;  assume we've reached the end of the hierarchy
                break
            if geo_level == "block group":
                geo_level = "blockgroup"
            geo_levels.append(geo_level)
            if geo_level == "county":
                first_county = data["county"][0]
                in_ += f" county:{first_county}"
        return {
            "states": states,
            "geo_levels": geo_levels
        }
    
    def api_get(self, payload):
        api_url = f'https://api.census.gov/data/{self.dataset_name}'
        return census_api_get(api_url, payload)

    def get_data(self, fields: list[str], states: list[str], geo_level: str, in_: str = ""):
        if geo_level == "blockgroup":
            geo_level = "block group"
        assert(fields)
        payload = {
            'get': ','.join(fields),
            'for': f'{geo_level}:{",".join(states) if geo_level == "state" else "*"}'
        }
        if in_:
            payload['in'] = in_
        
        data = self.api_get(payload)

        conversion_exceptions = []

        for col in data.columns:
            var_info = self.variables.get(col)
            if var_info:
                dtypes = {
                    'int': pd.Int32Dtype(), # NA-able (nullable) integer type
                    'string': object,
                    'float': np.float32,
                }
                if 'predicateType' not in var_info:
                    raise RuntimeError(
                        f"var_info for {self.dataset_name}.{col} is missing predicateType\n"
                        f"var_info = {var_info}"
                    )

                new_dtype = dtypes[var_info['predicateType']]
                if data[col].dtype != new_dtype:
                    required_type = dtypes[var_info['predicateType']]
                    try:
                        data[col] = data[col].astype(required_type)
                    except TypeError as e:
                        self.bad_cols[col] = data[col]
                        conversion_exceptions.append(
                            f"Cannot convert {self.dataset_name}.{col} to type {required_type}\n"
                            f"(Column stored as self.bad_cols[{col}] for developer inspection)\n"
                            f"Source data from census API contains:\n"
                            f"{data[col].map(lambda x: type(x)).value_counts().to_string()}\n"
                            f"(Exception: {e})")
                null_count = data[col].isna().sum()
                if null_count:
                    print(
                        "Warning: "
                        f"{self.dataset_name}.{col} {geo_level} contains {null_count} NULLs of {len(data[col])} values\n"
                        f"when fetched using ds.api_get({repr(payload)})")
    
        if conversion_exceptions:
            raise RuntimeError("----------------\n".join(conversion_exceptions))
        
        if "block group" in data.columns:
            data.rename(columns={"block group": "blockgroup"}, inplace=True)

        if "GEO_ID" in data.columns:
            data['geoid'] = data['GEO_ID'].str[9:]
        return data
    
    @cache
    def get_states(self) -> dict[str, dict]:
        states = self.get_data(["NAME", "GEO_ID"], ["*"], "state")
        return dict(sorted(zip(states["geoid"], states.to_dict('records'))))

    @cache
    def get_counties(self):
        counties = self.get_data(["NAME", "GEO_ID"], list(self.get_states().keys()), "county")
        return dict(sorted(zip(counties["geoid"], counties.to_dict('records'))))

    def get_county_fips_for_state(self, state: str):
        assert len(state) == 2
        return [county['geoid'][2:] for geoid, county in self.get_counties().items() if geoid.startswith(state)]

    def sql_table_name(self, table_name: str, geo_level: str):
        tokens = self.dataset_name.split("/")
        assert(len(tokens) == 3)
        (year, dataset, subfile) = tokens
        return sanitize_table_name(f"{self.schema_name}.{dataset}{year}{subfile}_{table_name}_{geo_level}")
    
    # Download all records for table_name
    def download_table(self, table_name: str, geo_level: str):
        sql_table_name = self.sql_table_name(table_name, geo_level)
        #print(f"Downloading {sql_table_name}")
        engine().execute(f"CREATE SCHEMA IF NOT EXISTS {get_schema(sql_table_name)}")

        assert table_name in self.tables
        table_coverage = self.get_table_coverage(table_name)
        states = table_coverage['states']
        assert geo_level in table_coverage['geo_levels']
        # fields = []
        # for var_name, var_info in self.tables[table_name].items():
        #     fields.append(var_name)
        #     fields += var_info['attributes'].split(',')
        # shards = []
        if geo_level in ['tract', 'blockgroup', 'block']:
            downloads = [{
                "sql":f"geoid between '{geoid}' and '{geoid}z'",
                "in":f"state:{geoid} county:{','.join(self.get_county_fips_for_state(geoid))}",
                "min_geoid": geoid,
                "max_geoid": f"{geoid}z"
            } for geoid in states]
        elif geo_level in ['county']:
            downloads = [{
                "in":f"state:{','.join(states)}"
            }]
        else:
            downloads = [{}]
        
        # Create tqdm progress bar that's initially not displayed
        pbar =  None

        for i, download in enumerate(downloads):
            sql = f"SELECT geoid from {sql_table_name}"
            if "sql" in download:
                sql += f" WHERE {download['sql']}"
            sql += " LIMIT 1"
            in_ = download.get("in", "")
            if engine().table_exists(sql_table_name) and len(engine().execute_returning_dicts(sql)) > 0:
                #print(f"{sql_table_name} {in_} already loaded ({count} records), skipping")
                if pbar is not None:
                    pbar.update()
                continue

            if pbar is None:
                pbar = tqdm(total=len(downloads), desc=sql_table_name, initial=i)

            # Run this long blocking call in a separate process to avoid blocking the main GIL
            PrCall(
                self.get_data_and_insert, 
                table_name, geo_level, sql_table_name, in_,
                download.get("min_geoid", None), download.get("max_geoid", None)
                ).value()
            pbar.update()
    
        self.add_primary_key(sql_table_name)

        if pbar is not None:
            pbar.close()

    def get_data_and_insert(self, table_name, geo_level, sql_table_name, in_, min_geoid, max_geoid):
        table_coverage = self.get_table_coverage(table_name)
        table = self.get_data([f"group({table_name})"], table_coverage["states"], geo_level, in_)
        sanitize_column_names(table, inplace=True)
        if min_geoid is not None:
            out_of_bounds = table[~table["geoid"].between(min_geoid, max_geoid)]
            if len(out_of_bounds) > 0:
                raise RuntimeError(
                    f"{table_name} {geo_level} Out of bounds geoids: {out_of_bounds['geoid']}")
        try:
            table.to_sql(get_table_name(sql_table_name), engine().engine, schema=get_schema(sql_table_name), if_exists='append', index=False)
        except (sqlalchemy.exc.IntegrityError, psycopg2.errors.UniqueViolation) as e:
            print(f"While get_data_and_insert for {sql_table_name}, in_ {in_}, got exception {e}", flush=True)
            raise e
        self.add_primary_key(sql_table_name)

    def add_primary_key(self, sql_table_name):
        if not engine().table_has_primary_key(sql_table_name):
            try:
                engine().execute(f'ALTER TABLE {sql_table_name} ADD PRIMARY KEY (geoid)')
            except (sqlalchemy.exc.IntegrityError, psycopg2.errors.UniqueViolation) as e:
                if engine().table_column_exists(sql_table_name, 'popgroup'):
                    engine().execute(f'ALTER TABLE {sql_table_name} ADD PRIMARY KEY (geoid, popgroup)')
                else:
                    raise RuntimeError(f"Table {sql_table_name} has duplicate geoids, but no popgroup column.")

    def download_table_geo_levels(self, table_name: str):
        try:
            geo_levels = self.get_table_coverage(table_name)["geo_levels"]
        except NoDataException as e:
            sys.stderr.write(f"Skipping {table_name} for all geo levels due to NoDataException\n")
            self.all_exceptions.append(e)
            return
        for geo_level in self.get_table_coverage(table_name)["geo_levels"]:
            self.download_table(table_name, geo_level)

    def download_tables_geo_levels(self, nthreads: int = 15):
        self.all_exceptions = []
        print(f"Downloading tables {', '.join(self.tables.keys())}", flush=True)
        if nthreads == 1:
            for table_name in self.tables.keys():
                self.download_table_geo_levels(table_name)
        else:
            pool = SimpleThreadPoolExecutor(nthreads)
            for table_name in self.tables.keys():
                 pool.submit(self.download_table_geo_levels, table_name)
            pool.shutdown(tqdm=tqdm(desc=self.dataset_name, colour="red"))
        if self.all_exceptions:
            print("Downloaded everything except for missing data as follows:")
            print("\n".join(map(str, self.all_exceptions)))

def display_storage():
    bar = None
    while True:
        size = engine().list_schema_sizes().query('schema_name == "census"')['size_mb'].iloc[0]*1e6
        if bar is None:
            bar = tqdm(desc="census schema size", colour="red", unit="B", initial=size, unit_scale=True)
        else:
            bar.update(size - bar.n)
        time.sleep(60)