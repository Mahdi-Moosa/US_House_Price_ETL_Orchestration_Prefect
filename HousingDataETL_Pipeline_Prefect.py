#!/usr/bin/env python
# coding: utf-8
# Steps of the data pipline for the US house data warehouse is described in: https://github.com/Mahdi-Moosa/US_Housing_Price_ETL


import requests, os, time
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import configparser
from functools import reduce
from datetime import datetime

start_time = datetime.now()

@task(log_prints=True, retries=3)
def fetch_file(url, folder_name, presence_check=True,  fname=None):
    """ Fetches file from URL.
    Input:
      folder_name: Folderpath where downloaded files will be saved (or looked for, if presence_check is True).
      fname: File name to save If not provided, file name is derived from the provided URL. String part after the last '/' in the url.
      url: Url to download.
      presence_check: When true - Will check for the file's presence and if present, file won't be downloaded. Default value is true. If you want to overwrite existing value, please pass False to this parameter.
    
    Return:
      Saves file at the specified filepath. When presence_check is True and file is already present, prints "File already present." statement.
    """  
    if fname is None:
        fname = url.split('/')[-1]
        fname = fname.split('?')[0]
        
    fpath = folder_name + '/' + fname

    if presence_check and os.path.exists(f'{fpath}'): #TODO: File size check validation.
        return print('File already present.')

    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)

    response = requests.get(url, stream=True)
    time.sleep(2)
    total_length = response.headers.get('content-length')
    total_length = round(int(total_length)/1e6,2)
    
    print(f'To be saved as {fpath}. Total size to be written is: {total_length} MB') 

    with open(fpath, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    
    if os.path.exists(f'{fpath}'): #TODO: File size check validation.
        return print(f'File download succesful for {fname}')

@task(log_prints=True)
def transform_UAD(raw_data_path_UAD : str = 'data/raw_data/FHFA-UAD/UADAggs_tract.zip', 
                        raw_data_path_zipcode_crosswalk : str = 'data/raw_data/HUD-USPS/TRACT_ZIP_122021.xlsx', 
                        raw_data_path_census_tract_map : str = 'data/raw_data/census/tab20_tract20_tract10_natl.txt'
                        )  -> pd.DataFrame:
    """ 
    This function will read raw UAD file from FHFA, raw tract to zipcode crosswalk file from UAD and raw tract 2010 to tract 2020 mapping file.
    The function will transform these raw files into a single file that will provide real estate asset apprisal by zipcode.
    The function will return a pandas dataframe of the final apprisal file.
    Input:
        raw_data_path_UAD: 
        raw_data_path_zipcode_crosswalk:
        raw_data_path_census_tract_map:
    Returns:
        pandas dataframe of the transformed data.
    """
    # Load UAD data, drop rows with Null appriasal values in the VALUE column.
    uad_df = pd.read_csv(raw_data_path_UAD)
    print('UAD data loaded successfully.')
    uad_df = uad_df[uad_df.VALUE.notna()]

    # Load tract to zip code data, drop one-to-many relationships
    tract_2_zip = pd.read_excel(raw_data_path_zipcode_crosswalk)
    tract_2_zip = tract_2_zip.drop_duplicates(subset='tract')
    print('Mapping data from tract_2010 to zip code loaded.')

    # Load tract 2010 to tract 2020 map, select relevant colums and drop one-to-many relationships
    tract_2010_to_tract_2020 = pd.read_csv(raw_data_path_census_tract_map, sep='|')
    print('Census tract 2020 to census tract 2010 map loaded.')


    # Data selection, drop duplicates
    tract_2020_2010_map = tract_2010_to_tract_2020[['GEOID_TRACT_20','GEOID_TRACT_10']]
    tract_2020_2010_map = tract_2020_2010_map.drop_duplicates(subset='GEOID_TRACT_20')

    # Maping census tract 2020 to census tract 2010.
    uad_df = uad_df.merge(tract_2020_2010_map, left_on='TRACT', right_on='GEOID_TRACT_20', how='left')

    # Dropping Null entries on GEOID_TRACT_20, if any
    uad_df = uad_df[uad_df.GEOID_TRACT_20.notna()]

    # Column data type assignment

    uad_df.GEOID_TRACT_10 = uad_df.GEOID_TRACT_10.astype('int64')
    uad_df.GEOID_TRACT_20 = uad_df.GEOID_TRACT_20.astype('int64')


    # ## Census Track to zip map
    # Mapping Zip to Census tract 2010
    uad_df_merged = uad_df.merge(tract_2_zip[['tract', 'zip']], left_on = 'GEOID_TRACT_10', right_on='tract', how='left')

    # Dropping null Zip entries.
    uad_df_merged = uad_df_merged[uad_df_merged.zip.notna()].copy(deep=True)

    # Data type conversion
    uad_df_merged.zip = uad_df_merged.zip.astype(int)

    # Dropping dupllicate columns
    uad_df_merged.drop(columns='tract', inplace=True)

    # Selecting only relevant columns
    uad_df_merged_slice_to_save =  uad_df_merged[['SERIESID', 'PURPOSE', 'YEAR', 'VALUE', 'GEOID_TRACT_10', 'GEOID_TRACT_20', 'zip']]

    # Filtering dataframe based on relevant SERIESIDs.
    uad_df_merged_slice_to_save = uad_df_merged_slice_to_save[uad_df_merged_slice_to_save.SERIESID.isin(['COUNT', 'MEDIAN', 'P25', 'P75', 'MEAN'])]

    # Splitting UAD table into count, mean, median, p25 and p75 tables
    count_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                            & (uad_df_merged_slice_to_save.SERIESID == 'COUNT')]

    mean_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                            & (uad_df_merged_slice_to_save.SERIESID == 'MEAN')]

    median_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                            & (uad_df_merged_slice_to_save.SERIESID == 'MEDIAN')]

    p25_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                            & (uad_df_merged_slice_to_save.SERIESID == 'P25')]

    p75_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                            & (uad_df_merged_slice_to_save.SERIESID == 'P75')]

    #Performing appropriate aggregation operation on count, mean, median, p25 and p75 tables. 

    zip_count_table = count_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).sum('VALUE').rename(columns={'VALUE':'VALUE_count'})
    zip_mean_table = mean_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_mean'})
    zip_median_table = median_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_median'})
    zip_p25_table = p25_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_p25'})
    zip_p75_table = p75_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_p75'})                            


    # Combining zip count, mean, median, p25 and p75 tables into single table where UAD data is turned into single table. This table is the end table of the ETL process for UAD data
    # from functools import reduce

    dfs_to_merge = [zip_count_table, zip_mean_table, zip_median_table, zip_p25_table, zip_p75_table]

    zip_uad_df_merged = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                                how='left'), dfs_to_merge)
    
    return zip_uad_df_merged


@task(log_prints=True, retries=3)
def save_pd_to_parquet(dtframe : pd.DataFrame, fldr_name : str, table_name : str):
    """
    This function saves dataframe as parquet file at specified folder locations. 
    Input:
        fldr_name: Folder name where data will be saved. Sub-directory supported. For example, you can specificy "destination_folder" or you can specify "destination_folder/yet_another_folder".
        table_name: This is the table name for parquet file(s). Data will be saved in a subdirectory under specified fldr_name with actual .parquet file with a timestamp. For example, 
            if table name is specified as "abc" then the folder organization will be 
                        | - fldr_name
                        | -- abc
                        | ---- abc_{os.timestamp}.parquet
        dtframe: Input dataframe.
    Return(s):
        print statement saying data write was sucessful.
    """
    import os
    from datetime import datetime
    cur_time = str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    file_path = fldr_name + '/' + table_name
    if not os.path.isdir(file_path):
        os.makedirs(file_path)
    dtframe.to_parquet(path=f'{fldr_name}/{table_name}/{table_name}_{cur_time}.parquet')
    return print(f'Table:  {table_name} saved.') 

@task(log_prints=True)
def transform_realtor(raw_data_path_realtor : str = 'data/raw_data/realtor_data/RDC_Inventory_Core_Metrics_Zip_History.csv') -> pd.DataFrame:
    # Reading and transforming data from downoaded zip file
    realtor_df = pd.read_csv(raw_data_path_realtor, low_memory=False)
    print('Realtor.com data successfully read from the .csv file.')
    realtor_df = realtor_df.iloc[0:-1] # Dropping last line that contains aggregated summary (line Total).

    # Selecting relevant columns.
    realtor_cols = ['month_date_yyyymm', 'postal_code', 'median_listing_price', 'average_listing_price', 'active_listing_count', 'median_days_on_market', 'new_listing_count', 
                    'price_increased_count', 'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot', 'median_square_feet', 'total_listing_count', 
                'pending_ratio', 'quality_flag']

    realtor_df_slice = realtor_df[realtor_cols].copy(deep=True)

    # Data format conversion
    realtor_df_slice.month_date_yyyymm = pd.to_datetime(realtor_df_slice.month_date_yyyymm, format='%Y%m') #.month_date_yyyymm.astype('datetime64[ns]')
    realtor_df_slice['postal_code'] = realtor_df_slice.postal_code.astype(int)


    # Zip-Year aggregation
    realtor_df_slice_agg = realtor_df_slice.groupby(by=['postal_code', realtor_df_slice.month_date_yyyymm.dt.year]).agg(
                                                                                                median_list_price = ('median_listing_price', 'mean'),
                                                                                                avg_list_price = ('average_listing_price', 'mean'),
                                                                                                active_list_count = ('active_listing_count', 'sum'),
                                                                                                median_DOM = ('median_days_on_market', 'mean'),
                                                                                                new_list_count = ('new_listing_count', 'sum'),
                                                                                                price_increase_count = ('price_increased_count', 'sum'),
                                                                                                price_reduced_count = ('price_reduced_count', 'sum'),
                                                                                                pending_list_count = ('pending_listing_count', 'sum'),
                                                                                                median_list_price_per_square_foot = ('median_listing_price_per_square_foot', 'mean'),
                                                                                                median_square_feet = ('median_square_feet', 'mean'),
                                                                                                total_list_count = ('total_listing_count', 'sum'),
                                                                                                pending_ratio = ('pending_ratio', 'mean')
                                                                                                )

    return realtor_df_slice_agg


@task(log_prints = True)
def transform_redfin(raw_data_path_redfin : str = 'data/raw_data/redfin_data/zip_code_market_tracker.tsv000.gz') -> pd.DataFrame:
    # Reading redfin data from downloaded compressed file.
    # Choosing relevant columns
    column_subset = ['period_end', 'property_type', 'median_sale_price', 'median_list_price', 
                    'median_ppsf', 'homes_sold', 'pending_sales', 'new_listings',  'inventory',
                    'avg_sale_to_list', 'region' ]
    
    redfin_df = pd.read_csv(raw_data_path_redfin, sep='\t', usecols=column_subset)

    # Data reformating/ type conversion
    redfin_df['zip'] = redfin_df.region.str.split(': ', expand=True)[1].astype('int')
    redfin_df.period_end = pd.to_datetime(redfin_df.period_end)

    # Aggregating data on zip code, year.
    redfin_df_zip_agg = redfin_df.groupby(by=['zip', redfin_df.period_end.dt.year]).agg(  median_sale_price = ('median_sale_price', 'mean'),
                                                                                                            median_list_price = ('median_list_price', 'mean'),
                                                                                                            median_ppsf = ('median_ppsf', 'mean'),
                                                                                                            homes_sold = ('homes_sold', 'sum'),
                                                                                                            pending_sales = ('pending_sales', 'sum'),
                                                                                                            new_listings = ('new_listings', 'sum'),
                                                                                                            inventory = ('inventory', 'sum'),
                                                                                                            avg_sale_to_list = ('avg_sale_to_list', 'mean')
                                                                                                        )
    return redfin_df_zip_agg

@task(log_prints = True)
def transform_zillow(raw_data_path_zillow : str = 'data/raw_data/zillow_data/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv') -> (pd.DataFrame, pd.DataFrame):
    # Reading zillow Data from downloaded file
    import pandas as pd
    zillow_zhvi_df = pd.read_csv(raw_data_path_zillow)
    print("zillow data read succesful.")

    # Selecting columns of interest
    excluded_cols = ['RegionID', 'SizeRank','StateName', 'State','RegionType' ]
    zillow_zhvi_df_slice = zillow_zhvi_df[[x for x in zillow_zhvi_df.columns if x not in excluded_cols]].copy(deep=True)
    zillow_zhvi_df_slice = zillow_zhvi_df_slice.set_index(['RegionName', 'City', 'Metro', 'CountyName']).stack().reset_index()
    zillow_zhvi_df_slice.columns = ['zip', 'city', 'metro', 'county', 'date', 'zhvi_usd_dominated']

    # Data type conversion
    zillow_zhvi_df_slice.date = pd.to_datetime(zillow_zhvi_df_slice.date)

    # Aggregate data based on zip code and year
    zillow_zhvi_zip_agg = zillow_zhvi_df_slice.groupby(by=['zip', zillow_zhvi_df_slice.date.dt.year]).mean()

    # Get zipcode to city-metro map
    zillow_zhvi_df_slice_zip_unduplicated = zillow_zhvi_df_slice.drop_duplicates(subset=['zip'])

    return zillow_zhvi_zip_agg, zillow_zhvi_df_slice_zip_unduplicated

@task(log_prints = True)
def transform_zipcode(raw_data_path_zipcode : str = 'data/raw_data/geonames/allCountries.zip') -> pd.DataFrame:
    # Read data
    global_postcode_data = pd.read_csv(raw_data_path_zipcode, sep='\t', low_memory=False, header=None)
    column_headers = ['country_code', 'postal_code', 'place_name', 'admin_name1', 'admin_code1', 'admin_name2', 'admin_code2', 'admin_name3', 'admin_code3', 'latitude', 'longitude', 'accuracy']
    global_postcode_data.columns = column_headers

    # Getting only US zip codes
    us_postal_codes = global_postcode_data[global_postcode_data.country_code == 'US'].copy(deep=True)

    # Selecting relevant columns and renaming for clarity.
    us_postal_codes = us_postal_codes[['postal_code', 'place_name', 'admin_code1', 'latitude', 'longitude', 'accuracy']]
    us_postal_codes.columns = ['zip', 'city', 'state', 'latitude', 'longitude', 'accuracy']

    # Data type conversion
    us_postal_codes.zip = us_postal_codes.zip.astype(int)
    print('Transformation of us postal code data was successful.')
    return us_postal_codes

@flow(name="UAD table ETL",
        description = "This sub-flow downloads house apprisal data from FHFA, census tract-to-zicpode crosswalk data from HUD and census tract 2010 to census tract 2020 map from Census.gov. Transforms data and saves ETL table as parquet file.",
        log_prints = True
        )
def uad_table_etl(file_with_configs : str = 'hpa.cfg') -> pd.DataFrame:
    # Get UAD urls to download
    config = configparser.ConfigParser()
    config.read(file_with_configs)
    uad_url = config.get('UAD', 'uad_url')
    tract_2_zip_url = config.get('UAD', 'tract_2_zip_url')
    tract20_10_map_url = config.get('UAD', 'tract20_10_map_url')
    zip_url = config.get('UAD', 'zip_url')

    # Download URLs 
    raw_data_folder = 'data/raw_data/'
    fetch_file(folder_name= raw_data_folder + 'FHFA-UAD',url=uad_url)
    fetch_file(folder_name= raw_data_folder + 'HUD-USPS', url=tract_2_zip_url)
    fetch_file(folder_name= raw_data_folder + 'census', url=tract20_10_map_url)
    fetch_file(folder_name= raw_data_folder + 'geonames', url=zip_url)

    zip_uad_df_merged = transform_UAD()

    uad_etl_data_folder = 'data/etl_data/uad_appraisal'
    save_pd_to_parquet(dtframe=zip_uad_df_merged, fldr_name=uad_etl_data_folder, table_name='zip_uad_table')
    print('ETL of UAD table was successful. Table saved.')
    return zip_uad_df_merged

@flow(name="Final tables ETL",
        description = "This sub-flow downloads redfin data, performs etl and saves transformed realtor etl data.",
        log_prints = True
        )
def house_price_tables(file_with_configs : str = 'hpa.cfg') -> None:
    config = configparser.ConfigParser()
    config.read(file_with_configs)

    zip_uad_df_merged = uad_table_etl()

    realtor_url = config.get('REALTOR', 'realtor_url')
    fetch_file(url=realtor_url, folder_name='data/raw_data/realtor_data/')
    realtor_df_slice_agg = transform_realtor()

    redfin_url = config.get('REDFIN', 'redfin_url')
    fetch_file(url=redfin_url, folder_name='data/raw_data/redfin_data/')
    redfin_df_slice_zip_agg = transform_redfin()

    zillow_url = config.get('ZILLOW', 'zillow_url')
    fetch_file(url=zillow_url, folder_name='data/raw_data/zillow_data/', fname='Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    zillow_zhvi_zip_agg, zillow_zhvi_df_slice_zip_unduplicated = transform_zillow()

    # Harmonizing column headers for joining
    multi_index_names = ['zip', 'year']
    zip_uad_df_merged.index.names = multi_index_names
    redfin_df_slice_zip_agg.index.names = multi_index_names
    realtor_df_slice_agg.index.names = multi_index_names
    zillow_zhvi_zip_agg.index.names = multi_index_names

    # Adding suffix to column names to keep track of the source in the merged dataframe
    zip_uad_df_merged = zip_uad_df_merged.add_suffix('_uad')
    redfin_df_slice_zip_agg = redfin_df_slice_zip_agg.add_suffix('_redfin')
    realtor_df_slice_agg = realtor_df_slice_agg.add_suffix('_realtor')
    zillow_zhvi_zip_agg = zillow_zhvi_zip_agg.add_suffix('_zillow')

    # Merging dataframe
    dfs_to_merge = [zip_uad_df_merged, redfin_df_slice_zip_agg, realtor_df_slice_agg, zillow_zhvi_zip_agg]
    zip_price_master_df = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                                how='outer'), dfs_to_merge)

    # Saving final table as parquet file
    out_dir = 'data/etl_data/zip_year_house_price_table'
    save_pd_to_parquet(dtframe=zip_price_master_df, fldr_name=out_dir, table_name='house_price_table')

    ## ETL of zipcode table.
    zip_url = config.get('ZIPCODE', 'zip_url') 
    fetch_file(url=zip_url, folder_name='data/raw_data/geonames/')
    us_postal_codes = transform_zipcode()
    
    # Adding Metro and City names (where present in the Zillow table)
    print(f'Columns available in the table: {zillow_zhvi_df_slice_zip_unduplicated.columns}')
    us_postal_codes_with_names = pd.merge(us_postal_codes, zillow_zhvi_df_slice_zip_unduplicated[[ 'zip','metro', 'county']], on='zip', how='left')

    # Saving final table as parquet file
    out_dir = 'data/etl_data/zipcode_table'
    save_pd_to_parquet(dtframe=us_postal_codes_with_names, fldr_name=out_dir, table_name='zipcode_table')

@flow(name="Main ETL Flow",
        description = "This flow orchestrates the house price ETL pipeline.",
        log_prints = True
    )
def house_price_etl_flow() -> None:
    """ The main ETL pipeline"""
    house_price_tables()
    print('House Price ETL complete. \nRelevant tables are saved as parquet file(s).')

if __name__ == "__main__":
    house_price_etl_flow()

print(f'Total run time = {datetime.now() - start_time}')