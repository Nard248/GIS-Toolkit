import os
import sys
import ast
import warnings
import shutil

import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from fiona.drvsupport import supported_drivers
import subprocess as sub

from sqlalchemy.orm import sessionmaker
import time
from datetime import datetime

warnings.filterwarnings('ignore')

supported_drivers['KML'] = 'rw'
supported_drivers['sqlite'] = 'rw'
supported_drivers['LIBKML'] = 'rw'


def query_footprint(state_abbr, fips, provider_id, tech_code, max_download, max_upload, br_code):
    query = f"""
        SELECT * FROM
        ww_get_all_cb_polygons_20(ARRAY[{state_abbr}], 
        ARRAY[{fips}], 
        ARRAY[{provider_id}], ARRAY[{tech_code}], 
        ARRAY[{max_download}], ARRAY[{max_upload}], 
        ARRAY[{br_code}]);
    """
    return query


def query_locations(state_abbr, fips):
    query = f"""
SELECT fcc.*                                 ,
       stags.categories_served_unserved      ,
       stags.categories_cd_ucd               ,
       stags.categories_mso                  ,
       stags.categories_cafii                ,
       stags.categories_rdof                 ,
       stags.categories_other_federal_grants ,
       stags.categories_unserved_and_unfunded,
       stags.categories_high_cost            ,
       stags.categories_fiber                ,
       uc.county_name                        ,
       tgs.wired_dl25_ul3_r                  ,
       tgs.wired_dl100_ul20_r                ,
       tgs.terrestrial_dl25_ul3_r            ,
       tgs.terrestrial_dl100_ul20_r          ,
       tgs.wiredlfw_dl25_ul3_r               ,
       tgs.wiredlfw_dl100_ul20_r             ,
       tgs.wired_dl25_ul3_b                  ,
       tgs.wired_dl100_ul20_b                ,
       tgs.terrestrial_dl25_ul3_b            ,
       tgs.terrestrial_dl100_ul20_b          ,
       tgs.wiredlfw_dl25_ul3_b               ,
       tgs.wiredlfw_dl100_ul20_b
    FROM us_sw2020_fabric_harvested_rel4_full fcc
        INNER JOIN fcc_bdc_fabric_rel4 stags ON fcc.fcc_location_id = stags.fcc_location_id
        LEFT JOIN us_sw2020_fabric_harvested_new_taggs tgs on fcc.fcc_location_id = tgs.location_id
        INNER JOIN us_counties uc ON fcc.fips_2020 = uc.fips_code
    WHERE fcc.state_abbr = ANY(ARRAY[{state_abbr}]) AND fcc.fips_2020 = ANY(ARRAY[{fips}])
    AND tgs.wiredlfw_dl25_ul3_r = 'U'
    AND tgs.wiredlfw_dl25_ul3_b = 'U'
    AND tgs.wiredlfw_dl100_ul20_r = 'U'
    AND tgs.wiredlfw_dl100_ul20_b = 'U'
    """
    return query


# def get_filtered_fips(provider_id, state, con):
#     query = f"""
#     SELECT DISTINCT fips_code
#     FROM us_census_block_data
#     WHERE provider_id = ANY(%s) AND state_abbr = ANY(%s)
#     """
#     print(query)
#     return pd.read_sql(query, con, params=(provider_id, state))
#
#
# def create_temp_table(con, fips_codes):
#     fips_codes.to_sql('temp_fips_codes', con, index=False, if_exists='replace', schema='public')
#

def query_counties_by_provider(provider_id, state, table_name):
    query = f"""
    SELECT uc.* 
    FROM us_counties uc
    INNER JOIN {table_name} temp ON temp.county_fips = uc.fips 
    """
    # WHERE fips IN
    #     (SELECT
    #         DISTINCT fips_code
    #      FROM us_census_block_data cbv
    #      WHERE cb.provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state}]))
    return query


def get_federal_grants(provider_id, state_abbr, con, table_name):
    query = f"""
SELECT info.*, gm.geometry FROM
(
    SELECT gt.id,
       ag.agency_name,
       ag.funding_program_name,
       ag.program_id,
       ag.category_buffer,
       gt.project_id,
       gt.project,
       gt.brandname,
       gt.providerid,
       gt.build_req,
       gt.loc_plan,
       gt.loc_sup,
       gt.technology_code,
       gt.technology_name,
       gt.maxdown,
       gt.maxup,
       uc.state_abbr,
       uc.county_name,
       uc.fips,
       gt.source,
       gt.source_dat,
       gt.categories_served
    FROM us_federal_grants gt
    INNER JOIN agencies ag ON gt.program_id = ag.program_id
    INNER JOIN federal_gt_counties_pivot pivot on gt.id = pivot.grant_id
    INNER JOIN us_counties uc on pivot.county_id = uc.id
    INNER JOIN {table_name} temp ON uc.fips = temp.county_fips
    WHERE uc.state_abbr = ANY(ARRAY[{state_abbr}])) info
INNER JOIN us_federal_grants_geometry gm ON info.id = gm.grant_id;
    """
    gdf = gpd.read_postgis(query, con, geom_col='geometry', crs='ESRI:102008')
    return gdf


def get_hex(provider_id, state, con, table_name):
    query = f"""
    SELECT h3.*
    FROM us_fcc_joined_h3_resolution8_test h3
    INNER JOIN {table_name} temp ON h3.county_fips = temp.county_fips
    """

    hex = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geom', crs='EPSG:4326').to_crs('ESRI:102008')

    return hex


def get_fip_codes(polygon_data, con, state_abbr):
    query = f"""
        SELECT state_abbr,fips,county_name
        FROM us_counties
        WHERE ST_Intersects(
            geom,
            ST_GeomFromText('{polygon_data.geometry.iloc[0]}', 102008)) AND state_abbr = ANY(ARRAY[{state_abbr}])
    """
    counties = pd.read_sql(query, con)
    return list(counties["fips"].unique())


def query_state(state_abbr):
    query = f"""
        SELECT * FROM "USStates" WHERE "StateAbbr" = ANY(ARRAY[{state_abbr}])
    """
    return query


def query_counties(fips):
    query = f"""
        SELECT * FROM us_counties WHERE fips = ANY(ARRAY[{fips}])
    """
    return query


def write_gradient_ranges_staticly(gdf, path=r'C:\OSGeo4W\processing_utilities'):
    range_dict = {
        "0": {"range": (1, 3), "color": "#e4e4f3"},
        "1": {"range": (3, 5), "color": "#dbdbee"},
        "2": {"range": (5, 7), "color": "#d1d1ea"},
        "3": {"range": (7, 10), "color": "#c8c8e6"},
        "4": {"range": (10, 15), "color": "#adadda"},
        "5": {"range": (15, 20), "color": "#9b9bd1"},
        "6": {"range": (20, 25), "color": "#8080c5"},
        "7": {"range": (25, 30), "color": "#6d6dbd"},
        "8": {"range": (30, 40), "color": "#5252b0"},
        "9": {"range": (40, 50), "color": "#4949ac"},
        "10": {"range": (50, 75), "color": "#4040a8"},
        "11": {"range": (75, 100), "color": "#3737a4"},
        "12": {"range": (100, 150), "color": "#2e2ea0"},
        "13": {"range": (150, 200), "color": "#24249c"},
        "14": {"range": (200, 300), "color": "#1b1b97"},
        "15": {"range": (300, 400), "color": "#121293"},
        "16": {"range": (400, 500), "color": "#09098f"},
        "17": {"range": (500, 50000), "color": "#00008b"}
    }

    max_number = gdf["Unserved_Unfunded"].unique().max()

    new_dict = dict()
    for key, value in range_dict.items():
        if max_number >= range_dict[key]["range"][0]:
            new_dict[key] = value
        else:
            break
    save_path = path + '/dict.txt'

    with open(save_path, 'w') as convert_file:
        convert_file.write(json.dumps(new_dict))

    return save_path


def mile_to_meter(miles):
    return miles * 1609.34


def create_formatted_excel(provider_name, market, unserved_unfunded_FP,
                           unserved_unfunded_10_miles, unserved_unfunded_30_miles,
                           in_footprint_counties, file_path, locations_within_counties, location_in_fp, state):
    df = pd.DataFrame({
        'Provider Name': [provider_name],
        'Market': [market],
        'Number of Unserved & Unfunded Locations in FP': [unserved_unfunded_FP],
        'Number of Unserved & Unfunded Locations in 10 Miles Buffer Ring': [unserved_unfunded_10_miles],
        'Number of Unserved & Unfunded Locations in 30 Miles Buffer Ring': [unserved_unfunded_30_miles],
        'In Footprint Counties': [in_footprint_counties]
    })

    # Start a writer instance using xlsxwriter
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Access the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets[f'{state}']

    # Define the formats
    header_format = workbook.add_format({'bg_color': '#9BBB59', 'bold': True})
    header_format2 = workbook.add_format({'bg_color': '#F79646', 'bold': True})
    provider_format = workbook.add_format({'bg_color': '#C5D9F1', 'bold': True})
    bold_format = workbook.add_format({'bold': True})
    light_blue_format = workbook.add_format({'bg_color': '#DAE8FC', 'bold': True})

    # Apply the formats to the header cells
    worksheet.write('A1', 'Provider Name', header_format)
    worksheet.write('B1', 'Market', header_format)
    worksheet.write('C1', 'Number of Unserved & Unfunded Locations in FP', header_format2)
    worksheet.write('D1', 'Number of Unserved & Unfunded Locations in 10 Miles Buffer Ring', header_format2)
    worksheet.write('E1', 'Number of Unserved & Unfunded Locations in 30 Miles Buffer Ring', header_format2)
    worksheet.write('F1', 'Number of Unserved & Unfunded Locations in Counties of FP', light_blue_format)  # New header

    # Apply bold format to 'Provider Name' and 'Market' values
    worksheet.write('A2', provider_name, provider_format)
    worksheet.write('B2', market, bold_format)
    worksheet.write('F2', in_footprint_counties, light_blue_format)  # New value

    # Set the column widths
    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 10)
    worksheet.set_column('C:C', 35)
    worksheet.set_column('D:E', 45)
    worksheet.set_column('F:F', 20)  # New column width

    # Calculate counts and merge dataframes
    locations_within_counties_counts = locations_within_counties['county_name'].value_counts().reset_index()
    locations_within_counties_counts.columns = ['County', 'Count of Locations']

    location_in_fp_counts = location_in_fp['county_name'].value_counts().reset_index()
    location_in_fp_counts.columns = ['County', 'Count of Locations in FP']

    high_cost_counts = locations_within_counties[locations_within_counties['categories_high_cost'] == True][
        'county_name'].value_counts().reset_index()
    high_cost_counts.columns = ['County', 'Count of High Cost']

    merged_df = locations_within_counties_counts.merge(location_in_fp_counts, on='County', how='left').merge(
        high_cost_counts, on='County', how='left')
    merged_df['Count of Locations in FP'] = merged_df['Count of Locations in FP'].fillna(0).astype(int)
    merged_df['Count of High Cost'] = merged_df['Count of High Cost'].fillna(0).astype(int)

    # Write the new DataFrame to a new sheet in the same Excel file
    merged_df.to_excel(writer, index=False, sheet_name='County Counts and High Cost')

    # Access the new worksheet
    merged_worksheet = writer.sheets['County Counts and High Cost']

    # Apply header format to the new worksheet
    merged_worksheet.write('A1', 'County Name', header_format)
    merged_worksheet.write('B1', 'Number of Unserved & Unfunded Locations in County', header_format)
    merged_worksheet.write('C1', 'Number of Unserved & Unfunded Locations in FP', header_format)
    merged_worksheet.write('D1', 'High Cost', header_format)

    # Set the column widths for the new worksheet
    merged_worksheet.set_column('A:A', 15)
    merged_worksheet.set_column('B:B', 20)
    merged_worksheet.set_column('C:C', 25)
    merged_worksheet.set_column('D:D', 20)

    # Close the Pandas Excel writer and output the Excel file
    writer.close()


def call_qgis_for_30_10(name_of_project, state_name, unserved_unfunded_in_fp, unserved_unfunded_10,
                        unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
                        cb_footprint_10, cb_footprint_30, provider_name, project_path, hex_layer, counties_footprint,
                        locations_in_counties, grad_path,  rdof_path, e_acam_path,
                                acam_path, ntia_path, rus_path, caf_ii_path, cpf_path, footprint_fiber_path, footprint_no_fiber_path):
    my_call = [r"C:\OSGeo4W\OSGeo4W.bat", r"python-qgis",
               r"C:\OSGeo4W\processing_utilities\save_30_10_buffer.py",
               name_of_project, state_name, unserved_unfunded_in_fp, unserved_unfunded_10,
               unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
               cb_footprint_10, cb_footprint_30, provider_name, project_path, hex_layer, counties_footprint,
               locations_in_counties, grad_path,  rdof_path, e_acam_path,
                                acam_path, ntia_path, rus_path, caf_ii_path, cpf_path, footprint_fiber_path, footprint_no_fiber_path]
    p = sub.Popen(my_call, stdout=sub.PIPE, stderr=sub.PIPE)
    stdout, stderr = p.communicate()

    print(stdout, stderr)
    return stdout, stderr


def log_time(log_file, message, start):
    end = time.time()
    elapsed_time = end - start
    log_message = f"{message}: {elapsed_time:.2f} seconds\n"
    print(log_message.strip())
    log_file.write(log_message)
    return end


def main():
    start_time = time.time()

    state_abbr = sys.argv[1]
    fips = sys.argv[2]
    provider_id = sys.argv[3]
    tech_code = sys.argv[4]
    max_download = sys.argv[5]
    max_upload = sys.argv[6]
    br_code = sys.argv[7]
    provider_name = sys.argv[8]
    path = sys.argv[9]
    state_name = sys.argv[10]
    file_type = sys.argv[11]

    # Create results directory
    step_start_time = time.time()
    results_dir = os.path.join(path, "results")

    if os.path.exists(results_dir):
        shutil.rmtree(results_dir)

    os.mkdir(results_dir)
    save_path = results_dir
    # step_start_time = log_time(None, "Creating results directory", step_start_time)

    # Open log file
    log_file_path = os.path.join(save_path, "log.txt")
    with open(log_file_path, "a") as log_file:
        log_file.write("Starting the Script of 30-10 Mile Buffer Version as of 01/31/2024 Version 8.0.0\n")

        # Database connection
        step_start_time = time.time()
        db_connection_url = "postgresql://postgresqlwireless2020:software2020!!@wirelesspostgresqlflexible.postgres.database.azure.com:5432/wiroidb2"
        con = create_engine(db_connection_url)
        step_start_time = log_time(log_file, "Establishing database connection", step_start_time)

        try:
            metadata = MetaData()

            # Create temporary table
            step_start_time = time.time()

            current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_name = f"temp_fips_{current_timestamp}"

            temp_fips_table = Table(
                table_name, metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('county_fips', String(255))
            )

            metadata.create_all(con)
            step_start_time = log_time(log_file, "Creating temporary table", step_start_time)

            # Insert data into temporary table
            step_start_time = time.time()
            Session = sessionmaker(bind=con)
            session = Session()

            insert_data_query = f"""
            INSERT INTO {table_name} (county_fips)
            SELECT DISTINCT fips_code
            FROM us_census_block_data cb
            WHERE provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state_abbr}]);
            """

            provider_id_var = ast.literal_eval(provider_id)
            if 130627 in provider_id_var:
                insert_data_query = f"""
                            INSERT INTO {table_name} (county_fips)
                            SELECT DISTINCT fips_code
                            FROM us_census_block_data cb
                            WHERE provider_id = ANY(ARRAY[{provider_id}]) AND cb.state_abbr = ANY(ARRAY[{state_abbr}]) AND cb.fips_code = ANY(ARRAY[{fips}]);
                            """

            session.execute(text(insert_data_query))
            session.commit()
            step_start_time = log_time(log_file, "Inserting data into temporary table", step_start_time)

            # Query footprint
            step_start_time = time.time()
            query_fp = query_footprint(state_abbr, fips, provider_id, tech_code, max_download, max_upload, br_code)
            footprint_raw = gpd.GeoDataFrame.from_postgis(query_fp, con, geom_col='Geometry', crs='EPSG:4326').to_crs(
                "ESRI:102008")

            footprint = footprint_raw.dissolve()
            ten_mile_buffer = footprint.buffer(mile_to_meter(10))
            ten_mile_dif = ten_mile_buffer.difference(footprint)
            thirty_mile_dif_raw = footprint.buffer(mile_to_meter(30))
            thirty_mile_dif = thirty_mile_dif_raw.difference(ten_mile_buffer)
            step_start_time = log_time(log_file, "Querying and processing footprint", step_start_time)

            # Get FIP codes
            step_start_time = time.time()
            fips = get_fip_codes(thirty_mile_dif_raw, con, state_abbr)
            step_start_time = log_time(log_file, "Getting FIP codes", step_start_time)

            # Query locations
            step_start_time = time.time()
            query_loc = query_locations(state_abbr, fips)
            locations = gpd.GeoDataFrame.from_postgis(query_loc, con, geom_col='geom', crs='EPSG:4326').to_crs(
                "ESRI:102008")
            locations = locations.drop_duplicates(subset=['fcc_location_id'], keep='last')
            step_start_time = log_time(log_file, "Querying locations", step_start_time)

            # Query state
            step_start_time = time.time()
            state = gpd.GeoDataFrame.from_postgis(query_state(state_abbr), con, geom_col='geometry',
                                                  crs='ESRI:102008').to_crs('EPSG:4326')
            step_start_time = log_time(log_file, "Querying state", step_start_time)

            # Query counties
            step_start_time = time.time()
            counties = gpd.GeoDataFrame.from_postgis(query_counties(fips), con, geom_col='geom',
                                                     crs='ESRI:102008').to_crs('EPSG:4326')
            counties_fp = gpd.GeoDataFrame.from_postgis(query_counties_by_provider(provider_id, state_abbr, table_name),
                                                        con, geom_col='geom', crs='ESRI:102008').to_crs('EPSG:4326')
            step_start_time = log_time(log_file, "Querying counties", step_start_time)

            # Spatial joins and processing
            step_start_time = time.time()
            locations_in_cb_footprint = gpd.sjoin(locations, footprint_raw, how="inner", predicate='intersects').to_crs(
                'EPSG:4326')
            locations_in_cb_footprint = locations_in_cb_footprint[locations.columns].drop_duplicates(
                subset='fcc_location_id', keep='last')

            locations_in_10mileBuffer = gpd.sjoin(locations, gpd.GeoDataFrame(geometry=ten_mile_dif), how="inner",
                                                  predicate='intersects').to_crs('EPSG:4326')
            locations_in_10mileBuffer = locations_in_10mileBuffer[locations.columns].drop_duplicates(
                subset='fcc_location_id', keep='last')

            locations_in_30mileBuffer = gpd.sjoin(locations, gpd.GeoDataFrame(geometry=thirty_mile_dif), how="inner",
                                                  predicate='intersects').to_crs('EPSG:4326')
            locations_in_30mileBuffer = locations_in_30mileBuffer[locations.columns].drop_duplicates(
                subset='fcc_location_id', keep='last')
            step_start_time = log_time(log_file, "Spatial joins and processing", step_start_time)

            # Convert CRS and overlay
            step_start_time = time.time()
            ten_mile_dif = ten_mile_dif.to_crs('EPSG:4326')
            thirty_mile_dif = thirty_mile_dif.to_crs('EPSG:4326')
            footprint = footprint.to_crs('EPSG:4326')
            ten_mile_dif = gpd.GeoDataFrame(geometry=ten_mile_dif)
            thirty_mile_dif = gpd.GeoDataFrame(geometry=thirty_mile_dif)
            ten_mile_dif = gpd.overlay(ten_mile_dif, state, how='intersection')
            thirty_mile_dif = gpd.overlay(thirty_mile_dif, state, how='intersection')
            step_start_time = log_time(log_file, "Converting CRS and overlay", step_start_time)

            # Hex grid processing
            step_start_time = time.time()

            hex_gdf = get_hex(provider_id, state_abbr, con, table_name=table_name)
            joined_gdf = gpd.sjoin(hex_gdf, locations, how="inner", predicate='contains')
            location_counts = joined_gdf.groupby(joined_gdf.index).size()
            hex_gdf['Unserved_Unfunded'] = \
                hex_gdf.merge(location_counts.rename('Unserved_Unfunded'), how='left', left_index=True,
                              right_index=True)[
                    'Unserved_Unfunded']
            hex_gdf = hex_gdf.dropna(subset=['Unserved_Unfunded'])
            hex_gdf['Unserved_Unfunded'] = hex_gdf['Unserved_Unfunded'].fillna(0)
            hex_gdf = hex_gdf[hex_gdf['Unserved_Unfunded'] > 0]
            hex_gdf = hex_gdf.to_crs('EPSG:4326')
            hex_gdf = hex_gdf.drop_duplicates(subset='h3_res8_id', keep='last')
            hex_gdf = hex_gdf.drop(
                columns=['frn', 'provider_id', 'brand_name', 'Technology Code', 'max_advertised_download_speed',
                         'max_advertised_upload_speed', 'low_latency', 'br_code', 'max_down_id', 'max_up_id', 'id',
                         'technology', 'state_abbr'])
            gradient_path = write_gradient_ranges_staticly(hex_gdf)
            step_start_time = log_time(log_file, "Hex grid processing", step_start_time)

            step_start_time = time.time()
            locations_within_counties = gpd.sjoin(locations, counties_fp.to_crs('ESRI:102008').drop(
                columns=['id', 'state_abbr', 'county_name']), how="inner", predicate='within').to_crs('EPSG:4326')
            locations_within_counties = locations_within_counties[locations.columns].drop_duplicates(
                subset='fcc_location_id', keep='last')
            step_start_time = log_time(log_file, "Processing locations within counties", step_start_time)

            # Federal grants
            step_start_time = time.time()
            federal_grants = get_federal_grants(provider_id, state_abbr, con, table_name=table_name).to_crs('EPSG:4326')

            clipped_geometries = []
            counties_fl = counties_fp.dissolve()
            for idx, row in federal_grants.iterrows():
                clipped_geometry = row['geometry'].intersection(counties_fl.unary_union)
                if not clipped_geometry.is_empty:
                    new_row = row.copy()
                    new_row['geometry'] = clipped_geometry
                    clipped_geometries.append(new_row)

            federal_grants = gpd.GeoDataFrame(clipped_geometries, crs=federal_grants.crs, geometry='geometry')
            step_start_time = log_time(log_file, "Processing federal grants", step_start_time)

            # File export
            step_start_time = time.time()
            extension = '.shp'
            if file_type == 'sqlite':
                extension = '.sqlite'
            elif file_type == 'kml':
                extension = '.kml'

            export_type_dict = {".shp": ['ESRI Shapefile'], '.sqlite': ['sqlite'], '.kml': ["KML"]}

            cb_footprint = os.path.join(save_path, f'{provider_name} CB Footprint{extension}')
            cb_footprint_10 = os.path.join(save_path, f'{provider_name} CB Footprint 10 Miles Buffer Ring{extension}')
            cb_footprint_30 = os.path.join(save_path, f'{provider_name} CB Footprint 30 Miles Buffer Ring{extension}')
            unserved_unfunded_in_fp = os.path.join(save_path, f'Unserved and Unfunded Locations in FP{extension}')
            unserved_unfunded_10 = os.path.join(save_path,
                                                f'Unserved and Unfunded Locations in 10 Miles Buffer Ring{extension}')
            unserved_unfunded_30 = os.path.join(save_path,
                                                f'Unserved and Unfunded Locations in 30 Miles Buffer Ring{extension}')
            state_polygon = os.path.join(save_path, f'{state_name} State Outline{extension}')
            county_polygon = os.path.join(save_path, f'{state_name} Counties Outline{extension}')
            hex_layer = os.path.join(save_path, f'Hex Layer of Unserved and Unfunded Locations{extension}')
            counties_footprint = os.path.join(save_path, f'{state_name} Counties of Footprint{extension}')
            locations_in_counties = os.path.join(save_path,
                                                 f'Unserved and Unfunded Locations in Counties of FP{extension}')
            federal_grants_path = os.path.join(save_path, f'Federal Grants in {state_name}{extension}')

            rdof_path, e_acam_path, acam_path, ntia_path, rus_path, caf_ii_path, cpf_path = '', '', '', '', '', '', ''

            rdof_dataframe = federal_grants[federal_grants['category_buffer'] == 'RDOF']
            e_acam_dataframe = federal_grants[federal_grants['category_buffer'] == 'E-ACAM']
            acam_dataframe = federal_grants[federal_grants['category_buffer'] == 'ACAM']
            ntia_dataframe = federal_grants[federal_grants['category_buffer'] == 'NTIA']
            rus_dataframe = federal_grants[federal_grants['category_buffer'] == 'RUS Reconnect']
            caf_ii_dataframe = federal_grants[federal_grants['category_buffer'] == 'CAF II']
            cpf_dataframe = federal_grants[federal_grants['category_buffer'] == 'CPF']

            if not rdof_dataframe.empty:
                rdof_path = os.path.join(save_path, f'RDOF Federal Grants in {state_name}{extension}')
                rdof_dataframe.to_file(rdof_path, driver=export_type_dict[extension][0])
            if not e_acam_dataframe.empty:
                e_acam_path = os.path.join(save_path, f'E-ACAM Federal Grants in {state_name}{extension}')
                e_acam_dataframe.to_file(e_acam_path, driver=export_type_dict[extension][0])
            if not acam_dataframe.empty:
                acam_path = os.path.join(save_path, f'ACAM Federal Grants in {state_name}{extension}')
                acam_dataframe.to_file(acam_path, driver=export_type_dict[extension][0])
            if not ntia_dataframe.empty:
                ntia_path = os.path.join(save_path, f'NTIA Federal Grants in {state_name}{extension}')
                ntia_dataframe.to_file(ntia_path, driver=export_type_dict[extension][0])
            if not rus_dataframe.empty:
                rus_path = os.path.join(save_path, f'RUS Reconnect Federal Grants in {state_name}{extension}')
                rus_dataframe.to_file(rus_path, driver=export_type_dict[extension][0])
            if not caf_ii_dataframe.empty:
                caf_ii_path = os.path.join(save_path, f'CAF II Federal Grants in {state_name}{extension}')
                caf_ii_dataframe.to_file(caf_ii_path, driver=export_type_dict[extension][0])
            if not cpf_dataframe.empty:
                cpf_path = os.path.join(save_path, f'CPF Federal Grants in {state_name}{extension}')
                cpf_dataframe.to_file(cpf_path, driver=export_type_dict[extension][0])

            footprint_fiber = footprint_raw[footprint_raw['TechCode'] == 50].to_crs('EPSG:4326').dissolve()
            footprint_no_fiber = footprint_raw[footprint_raw['TechCode'] != 50].to_crs('EPSG:4326').dissolve()

            footprint_fiber_path, footrpint_no_fiber_path = '', ''

            if not footprint_fiber.empty:
                footprint_fiber_path = os.path.join(save_path, f'Fiber Footprint in {state_name}{extension}')
                footprint_fiber.to_file(footprint_fiber_path, driver=export_type_dict[extension][0])
            if not footprint_no_fiber.empty:
                footrpint_no_fiber_path = os.path.join(save_path, f'Not Fiber Footprint in {state_name}{extension}')
                footprint_no_fiber.to_file(footrpint_no_fiber_path, driver=export_type_dict[extension][0])


            footprint.to_file(cb_footprint, driver=export_type_dict[extension][0])
            ten_mile_dif.to_file(cb_footprint_10, driver=export_type_dict[extension][0])
            thirty_mile_dif.to_file(cb_footprint_30, driver=export_type_dict[extension][0])
            locations_in_cb_footprint.to_file(unserved_unfunded_in_fp, driver=export_type_dict[extension][0])
            locations_in_10mileBuffer.to_file(unserved_unfunded_10, driver=export_type_dict[extension][0])
            locations_in_30mileBuffer.to_file(unserved_unfunded_30, driver=export_type_dict[extension][0])
            state.to_file(state_polygon, driver=export_type_dict[extension][0])
            counties.to_file(county_polygon, driver=export_type_dict[extension][0])
            hex_gdf.to_file(hex_layer, driver=export_type_dict[extension][0])
            counties_fp.to_file(counties_footprint, driver=export_type_dict[extension][0])
            locations_within_counties.to_file(locations_in_counties, driver=export_type_dict[extension][0])
            step_start_time = log_time(log_file, "Exporting files", step_start_time)

            # Call QGIS function
            step_start_time = time.time()
            call_qgis_for_30_10(f'{state_name} BufferedFootprint', state_name, unserved_unfunded_in_fp,
                                unserved_unfunded_10, unserved_unfunded_30, state_polygon, county_polygon, cb_footprint,
                                cb_footprint_10, cb_footprint_30, provider_name, save_path, hex_layer,
                                counties_footprint,
                                locations_in_counties, gradient_path, rdof_path, e_acam_path,
                                acam_path, ntia_path, rus_path, caf_ii_path, cpf_path, footprint_fiber_path, footrpint_no_fiber_path)
            step_start_time = log_time(log_file, "Calling QGIS function", step_start_time)

            # Create Excel report
            step_start_time = time.time()
            path_excel = os.path.join(save_path, f'{state_name}_NumberReport.xlsx')
            create_formatted_excel(provider_name, state_name, len(locations_in_cb_footprint),
                                   len(locations_in_10mileBuffer),
                                   len(locations_in_30mileBuffer), len(locations_within_counties), path_excel,
                                   locations_within_counties, locations_in_cb_footprint, state_name)
            step_start_time = log_time(log_file, "Creating Excel report", step_start_time)

            # Delete temporary table
            step_start_time = time.time()
            delete_table_query = f"DROP TABLE IF EXISTS {table_name};"
            session.execute(text(delete_table_query))
            session.commit()
            session.close()
            step_start_time = log_time(log_file, "Deleting temporary table", step_start_time)

        finally:
            con = 0

        total_time = time.time() - start_time
        total_time_message = f"Total execution time: {total_time:.2f} seconds\n"
        print(total_time_message.strip())
        log_file.write(total_time_message)


if __name__ == "__main__":
    main()
