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
    A class that manipulates Olist datasets. Default S3 bucket is ianyu-public-data
    '''
    def __init__(self, bucket = "ianyu-public-data"):
        self.bucket = bucket
        self.s3 = boto3.resource('s3')

    # Get table names
    def table_names(self, prefix = "olist/olist"):
        '''
        A function that gets all the available datasets given a prefix on S3 Bucket
        The default is the default prefix for "ianyu-public-data"
        The result is a dictionary with table names as keys and path as values 
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
        A function that takes in key argument and path argument
        Key argument indicates which dataset to request
        The function returns a Pandas DataFrame
        '''
        assert isinstance(clean, bool), "clean must be a boolean"
        if not self.tables:
            raise Exception("Tables must not be empty. Run Olist.table_names() to get table names")
        
        df = pd.read_csv(self.s3.Object(bucket_name = self.bucket, key = self.tables[key]).get()['Body'])
        if clean == False:
            pass
        elif clean == True:
            df = clean_df(df=df, key=key)
        
        return df


def clean_df(df, key):
    '''
    A function that contains a list of rulesets in order to clean Olist's dataset.
    It is made to have rulesets adjsuted according to needs.
    '''

    # A growing list of custom rules
    rule_list = [
        'marketing_qualified_leads',
        'closed_deals'
        ]

    # If a rule set is available, then pass and continue to clean the dataframe
    if key in rule_list:
        pass
    
    # Else raise an Exception as there is no rule to clean the data
    else:
        raise Exception(f"cleaning for {key} is not available yet")


    # Custom rules to clean Marketing Qualified Leads dataset
    if key == 'marketing_qualified_leads':
    
        # Make first_contact_date datetime datatype
        df['first_contact_date'] = pd.to_datetime(df['first_contact_date'])
        
        # Fill all NaN values with other
        df['origin'].fillna('other', inplace = True)

        # Replace all unknown values with other
        df['origin'] = df['origin'].map(lambda x: x.replace("unknown", "other"))

    
    # Custom rules to clean Closed Deals 
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
    
    else:
        pass
    
    return df

#####################################################################
# Post-Sales Analysis: Aims to be Developed Into a Post-Sales Class #
#####################################################################


def single_seller(ps, feature='price', seller_id = None, resample = '1D', function = 'cumsum'):
    '''
    A custom function that helps examine a seller's performance over time
    '''
    if seller_id == None:
        seller = random.choice([x for x in ps['seller_id']])
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