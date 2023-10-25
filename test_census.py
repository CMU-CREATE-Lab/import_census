from download_census_data import CensusApiDataset

all_states = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56', '72']
all_states_minus_puerto_rico = [state for state in all_states if state != '72']


def test_get_table_coverage():
    def test_coverage(table_name, expected_states, expected_geo_levels):
        coverage = ds.get_table_coverage_uncached(table_name)
        assert coverage['states'] == expected_states
        assert coverage['geo_levels'] == expected_geo_levels
        assert ds.get_table_coverage(table_name) == coverage

    # https://www2.census.gov/programs-surveys/acs/tech_docs/table_shells/table_lists/2021_DataProductList.xlsx
    ds = CensusApiDataset("2021/acs/acs5")

    # "B98013"  US only
    test_coverage("B98013", [], ['us'])

    # # "B07001PR" Puerto Rico geographies only	Excludes Place/Remainder and Block Group geographies
    # test_coverage("B07001PR", ['72'], ['state', 'county', 'tract'])

    # # # "B07001" Excludes Puerto Rico geographies  	Excludes Place/Remainder and Block Group geographies
    # test_coverage("B07001", all_states_minus_puerto_rico, ['state', 'county', 'tract'])

    # # # "B05003" All states.  Excludes Place/Remainder and Block Group geographies
    # test_coverage("B05003", all_states, ['state', 'county', 'tract'])

    # # # "B01001"  All states and all geographics
    # test_coverage("B01001", all_states, ['state', 'county', 'tract', 'blockgroup'])

    # # "B07201"  All states but Puerto Rico.  All geographics
    # test_coverage("B07201", all_states_minus_puerto_rico, ['state', 'county', 'tract', 'blockgroup'])

    # # "B98013"  US only
    # test_coverage("B98013", [], ['us'])

