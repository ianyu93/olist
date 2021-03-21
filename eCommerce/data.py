'''
2021.03.25
The purpose of data.py is to streamline an API to call Olist's datasets without downloading to local machine.
This also allows Users to easily create copies of the datasets. 
Further factorization will be required in future sprints.
'''
import boto3

# Public S3 Bucket to import from
bucket = "ianyu-public-data"
s3 = boto3.resource('s3')

# Set table variables
customers = s3.Object(bucket_name = bucket, key = "olist/olist_customers_dataset.csv").get()['Body']
geolocation = s3.Object(bucket_name= bucket, key = "olist/olist_geolocation_dataset.csv").get()['Body']
order_items = s3.Object(bucket_name= bucket, key = "olist/olist_order_items_dataset.csv").get()['Body']
order_payments = s3.Object(bucket_name= bucket, key = "olist/olist_order_payments_dataset.csv").get()['Body']
order_reviews = s3.Object(bucket_name= bucket, key = "olist/olist_order_reviews_dataset.csv").get()['Body']
orders = s3.Object(bucket_name= bucket, key = "olist/olist_orders_dataset.csv").get()['Body']
products = s3.Object(bucket_name= bucket, key = "olist/olist_products_dataset.csv").get()['Body']
sellers = s3.Object(bucket_name= bucket, key = "olist/olist_sellers_dataset.csv").get()['Body']
marketing_qualified_leads = s3.Object(bucket_name= bucket, key = "olist/olist_marketing_qualified_leads_dataset.csv").get()['Body']
closed_deals = s3.Object(bucket_name= bucket, key = "olist/olist_closed_deals_dataset.csv").get()['Body']

# Get table names
def get_tables():
    '''
    A function that calls all table names for Users to see all table names at once.
    Returns a list of table names.
    '''
    prefix_objs = s3.Bucket(bucket).objects.filter(Prefix="olist/olist")
    tables = [obj.key.replace("olist/olist_", "").replace("_dataset.csv","") for obj in prefix_objs]
    return tables