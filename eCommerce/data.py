'''
2021.03.25
The purpose of data.py is to streamline an API to call Olist's datasets without downloading to local machine.
This also allows Users to easily create copies of the datasets. 
Further factorization will be required in future sprints.
'''
import re
import boto3
import random
import pandas as pd

#######################
# Olist Data Pipeline #
#######################

class Olist:
    '''
    Manipulates Olist datasets. Default S3 bucket is ianyu-public-data.

    Parameters 
    ==========
    bucket: str, S3 bucket name to pull Olist data from. ianyu-public-data by default.

    Attributes
    ==========
    bucket: S3 bucket name
    s3: boto3.resource instance
    tables: all table names collected through 

    Methods
    =======
    table_names(): returns names of all available Olist datasets retrieved 
    get_table(key): returns a dataset for a given key 
    '''

    def __init__(self, bucket = "ianyu-public-data"):
        self.bucket = bucket
        self.s3 = boto3.resource('s3')

    # Get table names
    def table_names(self, prefix = "olist/olist"):
        '''
        Gets all the available datasets given a prefix on S3 Bucket
        The default is the default prefix for "ianyu-public-data"
        The result is a dictionary with table names as keys and path as values 

        Parameters
        ==========
        prefix: str, prefix on the S3 bucket where Olist's datasets exist
        '''
        # Get names 
        tables = {}
        prefix_objs = self.s3.Bucket(self.bucket).objects.filter(Prefix=prefix)
        for obj in prefix_objs:
            k = re.sub(r'(_dataset.csv)', "", re.sub(r'.*(olist_)', "", obj.key))
            v = obj.key
            tables[k] = v
        
        self.tables = tables

    def get_table(self, key, clean = False):
        '''
        Takes in key argument and path argument
        Key argument indicates which dataset to request
        The function returns a Pandas DataFrame

        Parameters
        ==========
        key: str, name of the dataset to retrieve
        clean: bool, if True, activate clean_df to to clean the dataset. False by default.
        '''
        assert isinstance(clean, bool), "clean must be a boolean"

        # Get table
        if not self.tables:
            raise Exception("Tables must not be empty. Run Olist.table_names() to get table names")
        
        df = pd.read_csv(self.s3.Object(bucket_name = self.bucket, key = self.tables[key]).get()['Body'])

        # Activate clean_df()
        if clean == False:
            pass
        elif clean == True:
            df = clean_df(df=df, key=key)
        
        return df


def clean_df(df, key):
    '''
    Contains a list of rulesets in order to clean Olist's dataset.
    It is made to have rulesets adjsuted according to needs.

    Parameters
    ==========
    df: pd.DataFrame, Olist DataFrame to clean
    key: str, name of the Olist dataset to clean
    '''

    # A growing list of custom rules
    rule_list = [
        'marketing_qualified_leads',
        'closed_deals'
        ]

    # If a rule set is available, then pass and continue to clean the dataframe
    if key not in rule_list:
        raise Exception(f"cleaning for {key} is not available yet")


    # Custom rules to clean Marketing Qualified Leads dataset
    if key == 'marketing_qualified_leads':

        # Make first_contact_date datetime datatype
        df['first_contact_date'] = pd.to_datetime(df['first_contact_date'])

        # Fill all NaN values with other
        df['origin'].fillna('other', inplace = True)

        # Replace all unknown values with other
        df['origin'] = df['origin'].map(lambda x: x.replace("unknown", "other"))


    elif key == 'closed_deals':
        '''
        Custom rules to clean closed deals
        '''
        # A list of columns to drop completely
        # Note: Only columns that normally has too much missing data
        drop_all = [
            'has_company', 
            'has_gtin', 
            'average_stock', 
            'declared_product_catalog_size',
            ]

        # A list of columns to only drop where there is NaN
        # Note: Only columns that even when drop NaN values, would not introduce too much bias
        drop_little = [
            'business_segment',
            'lead_type', 
            'business_type'
            ]

        # change won_date to datetime
        df['won_date'] = df['won_date'].astype("datetime64")

        # drop a given list of columns completely first, then find any data point with NaN values
        df = df.drop(drop_all, axis=1).dropna(how='any', subset=drop_little)

    return df

#####################################################################
# Post-Sales Analysis: Aims to be Developed Into a Post-Sales Class #
#####################################################################


def single_seller(ps, feature='price', seller_id = None, resample = '1D', function = 'cumsum'):
    '''
    Custom function that helps examine a seller's performance over time

    Parameters
    ==========
    feature: str, feature of the DataFrame to analyize. price by default.
    seller_id: int, seller_id. None by default, which triggers random selection of seller_id
    resample: str, timeframe to resample. 5M for 5 months, 1Y for 1 year. 1D by default.
    function: str, aggregate function. choice of daily_sum or daily_mean or cumsum or total_growth
    '''
    if seller_id is None:
        seller = random.choice(list(ps['seller_id']))
    else:
        seller = seller_id

    sum_ = pd.DataFrame(ps[ps['seller_id']==seller][feature].resample(resample).sum())

    if function == 'daily_sum':
        return sum_

    elif function == 'daily_mean':
        return sum_.mean()

    elif function == 'cumsum':
        return sum_.cumsum()

    elif function == 'total_growth':
        start = float(sum_.cumsum().iloc[0])
        end = float(sum_.cumsum().iloc[-1])
        return ((end - start) / start)

    else:
        raise Exception("Invalid function")