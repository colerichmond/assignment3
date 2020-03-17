
import pandas as pd
import os

def get_old_stops(year, old_columns, mid_columns):
    '''
    return a downloadable file of traffic
    stops for a given year before RIPA.
    
    :param: given year to fetch
    '''
    
    if year == '2018':
        url = 'http://seshat.datasd.org/pd/vehicle_stops_%s_datasd_v1.csv' % ('final')
        column_set = mid_columns
        
    else:
        url = 'http://seshat.datasd.org/pd/vehicle_stops_%s_datasd_v1.csv' % (year)
        column_set = old_columns

    #return pd.read_csv(url, index_col=0).set_index('stop_id').loc[:, column_set]
    return pd.read_csv(url, index_col=0).loc[:, column_set]

def get_new_stops(year):
    '''
    return a downloadable file of traffic
    stops post-RIPA.
    
    :param: given year to fetch
    '''
    
    url = 'http://seshat.datasd.org/pd/ripa_stops_datasd_v1.csv'
    #table = pd.read_csv(url).set_index('stop_id')
    table = pd.read_csv(url, index_col=0)

    return table.loc[~table.index.duplicated(keep='first')]

def add_ripa(table, new_columns, ripa_keys):
    '''
    obtains all necessary ripa attributes
    and joins them to new stops data.
    
    :param: ripa_keys: ripa attributes to collect
    '''
    
    for ripa_key in ripa_keys:
        url = 'http://seshat.datasd.org/pd/ripa_%s_datasd.csv' % (ripa_key)
        to_add = pd.read_csv(url).set_index('stop_id')
        to_add = to_add.loc[:, [col for col in to_add.columns if col != 'pid']]
        to_add = to_add.loc[~to_add.index.duplicated(keep='first')]
        table = pd.concat([table,to_add], axis=1)
    
    return table.loc[:, new_columns]

def get_clean_stops(pth, df, unwanted_cols):
    
    df = remove_unwanted(pth, df, unwanted_cols)
    
    if '2019' in pth:
        df = df.rename(columns={"beat": "service_area",
                                "perceived_age" : "subject_age",
                                "gend" : "subject_sex",
                                "race" : "subject_race",
                                "contraband" : "contraband_found",
                                "reason_for_stop" : "stop_cause",
                                "basis_for_search" : "searched",
                                "result" : "arrested"})
        df = clean_gender(df)
        df = clean_race(df)
        df = clean_contraband(df)
        df = clean_cause(df)
        df = clean_searched(df)
        df = clean_arrests(df)
    df = convert_date(pth, df)
     
    return df

def clean_arrests(df):
    
    df['arrested'] = df['arrested'].str.lower().apply(lambda basis: 'Y' if 'arrest' in basis else 'N')
    
    return df

def clean_searched(df):
    
    df['searched'] = df['searched'].apply(lambda basis: 'N' if pd.isnull(basis) else 'Y')
    
    return df

def clean_cause(df):
    
    df['stop_cause'] = df['stop_cause'].apply(lambda cause: 'Moving Violation' if cause in ['Reasonable Suspicion', 'Traffic Violation'] else 'Code Violation')
    
    return df

def clean_contraband(df):
    
    df['contraband_found'] = df['contraband_found'].apply(lambda x: 'N' if x is not None else 'Y')
            
    return df

def clean_race(df):
    
    new_race = {'White' : 'W',
                'Hispanic/Latino/a' : 'H',
                'Black/African American' : 'B',
                'Native American' : 'O',
                'Middle Eastern or South Asian' : 'A',
                'Asian' : 'A',
                'Pacific Islander' : 'P'}
    
    df = df.replace({"subject_race": new_race})
    
    return df

def clean_gender(df): 
    
    new_sex = {0: pd.np.nan,
               1: "M", 
               2: "F",
               3: pd.np.nan,
               4: pd.np.nan}
    
    df = df.replace({"subject_sex": new_sex})
    
    return df

def convert_date(pth, df):
    
    if '2018' in pth: 
        df['date_stop'] = pd.to_datetime(df['date_time'], format='%Y-%m-%d %H:%M:%S').apply(lambda datestamp: datestamp.date())
        df['time_stop'] = pd.to_datetime(df['date_time'], format='%Y-%m-%d %H:%M:%S').apply(lambda datestamp: datestamp.time())
        df['year'] = df['date_stop'].apply(lambda datestamp: datestamp.year)
        df['month'] = df['date_stop'].apply(lambda datestamp: datestamp.month)
        df['day'] = df['date_stop'].apply(lambda datestamp: datestamp.day)
        df['weekday'] = df['date_stop'].apply(lambda datestamp: datestamp.weekday())
        df['hour'] =  df['time_stop'].apply(lambda datestamp: datestamp.hour)
        df['minute'] =  df['time_stop'].apply(lambda datestamp: datestamp.minute)
        
    else:
        df['date_stop'] = pd.to_datetime(df['date_stop'], format='%Y-%m-%d', errors='coerce')
        df['time_stop'] = pd.to_datetime(df['time_stop'], format='%H:%M', errors='coerce')
        df['year'] = df['date_stop'].apply(lambda datestamp: datestamp.year)
        df['month'] = df['date_stop'].apply(lambda datestamp: datestamp.month)
        df['day'] = df['date_stop'].apply(lambda datestamp: datestamp.day)
        df['weekday'] = df['date_stop'].apply(lambda datestamp: datestamp.weekday)
        df['hour'] =  df['time_stop'].apply(lambda datestamp: datestamp.hour)
        df['minute'] =  df['time_stop'].apply(lambda datestamp: datestamp.minute)
        
        
    if 'date_time' in df.columns:
        df = df.drop(['date_time'], axis=1)
    
    return df

def remove_unwanted(pth, df, unwanted_cols):
    
    for col in unwanted_cols:  
        if col in df.columns:
            df = df.drop([col], axis=1)
            
    return df

# ---------------------------------------------------------------------
# Driver Function(s)
# ---------------------------------------------------------------------
    
def get_data(years, old_columns, mid_columns, new_columns, ripa_keys, outpath):
    '''
    downloads and saves traffic stops tables 
    at the specified output directory for the
    given year.
    
    :param: years: a list of years to collect
    :param: outdir: the directory to which to save the data
    '''
    print('Ingesting data...')
    if not os.path.exists(outpath):
        os.makedirs(outpath)
        
    for year in years:
        if year not in ['2019']:
            table = get_old_stops(year, old_columns, mid_columns)
            file_name = 'stops_%s.csv' % (year)
            table.to_csv(os.path.join(outpath, file_name))
        else:
            table = get_new_stops(year)
            ripa_table = add_ripa(table, new_columns, ripa_keys)
            file_name = 'stops_%s.csv' % (year)
            ripa_table.to_csv(os.path.join(outpath, file_name))
    
    print('...done!')
    return

def clean_stops(unwanted_cols, df_iter=(), outpath=None, inpath=None):
    
    print('Cleaning data...')
    
    if outpath and not os.path.exists(outpath):
        os.makedirs(outpath)

    if not df_iter:
        df_iter = ((p, pd.read_csv(os.path.join(inpath, p), index_col=0)) for p in os.listdir(inpath))

    for pth, df in df_iter:
        cleaned = get_clean_stops(pth, df, unwanted_cols)
        if outpath:
            cleaned.to_csv(os.path.join(outpath, pth))
            
    print('...done!')
            
    return 