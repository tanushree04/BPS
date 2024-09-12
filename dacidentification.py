# split df_combined_costar_small into dataframes with max. 9,999 rows
df_dict = {n: df_combined_costar_small.iloc[n:n+9999, :] 
    for n in range(0, len(df_combined_costar_small), 9999)}

# export dataframes for the census geocoder
for key in df_dict.keys():
    print(key)
    df_dict[key].to_csv(f'PropertyDataCombined_{key}.csv', index_label='ID')

### read new csvs with tracts from the census geocoder
dfs = []
for key in df_dict.keys():
    print(key)
    df_geolocater = pd.read_csv(f'geocoderesult_{key}.csv',
        names=[
            'Input Address',
            'TIGER Address Range Match Indicator',
            'TIGER Match Type',
            'TIGER Output Address',
            'LongitudeAndLatitude',
            'Tigerline ID',
            'Tigerline ID Side',
            'State',
            'County',
            'Tract',
            'Block'],
            dtype=str)
 
    dfs.append(df_geolocater)

df_tracts = pd.concat(dfs)

# clean up addresses and create census_tract_id column
df_tracts.replace({',': ''}, regex=True, inplace=True) # remove extra comma in address
df_tracts['full_address'] = df_tracts['Input Address'].str.lower() # convert to lowercase
df_tracts = normalize_common_address_comps(df_tracts, 'full_address')
df_tracts['State'] = df_tracts['State'].str.lstrip('0') # strip the 0 from the front of State values
df_tracts['census_tract_id'] = df_tracts.apply(lambda row: census_tract_id(row), axis=1) # create census id column

# filter to matched addresses
df_tracts_match = df_tracts.loc[df_tracts['TIGER Address Range Match Indicator'] == 'Match']
df_tracts_match_simple = df_tracts_match[['full_address', 'census_tract_id']]

# merge with maryland costar properties dataframe
df_md_costar_tracts = df_combined_costar.merge(df_tracts_match_simple, on='full_address', how='inner')