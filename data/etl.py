from email import header
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import SparkContext
from pathlib import Path
from pyspark import SparkFiles

# This is the root path for Download all the files
s3_path = 'https://ifood-data-architect-test-source.s3-sa-east-1.amazonaws.com/'

# I decided to have two list of urls, one for the files that are uniques
# and the other for the files that have incremental names 
files_extensions = ['status.json.gz', 'restaurant.csv.gz', 'consumer.csv.gz']
files_order_extensions = ["orders/part-{0:03}.json.gz".format(i) for i in range(2)]

paths = [f'{s3_path}{file}' for file in files_extensions]
order_paths = [f'{s3_path}{file}'  for file in files_order_extensions]

# Starting the script
if __name__ == "__main__":
    spark = SparkSession.builder.appName('etl').getOrCreate()
    sc=spark.sparkContext

    # Downloading the status, restaurant and consumer datasets and wrinting to raw data
    for path in paths:
        filename = path.split('/')[-1]
        output_path = filename.replace('.csv.gz', '').replace('.json.gz', '')
        last_path = raw_path / output_path

        sc.addFile(path)

        if path.endswith('csv.gz'):
            df = spark.read.csv(SparkFiles.get(filename), header=True, inferSchema= True)
        elif path.endswith('json.gz'):
            df = spark.read.json(SparkFiles.get(filename))
        else:
            print(f'Invalid file extension {s3_path}')
            continue

        df.write.parquet(f'raw/{output_path}')

    # Downloading all the order dataset and wrinting to raw data
    for order_path in order_paths:
        filename = order_path.split('/')[-1]
        output_path = filename.replace('.json.gz', '')

        sc.addFile(order_path)

        df = spark.read.json(SparkFiles.get(filename))
        df.write.parquet(f'raw/order/{output_path}')
    

    # Creating the order status dataset and wrinting to trusted data
    status = spark.read.parquet('file:///home/gerson/raw/status')
    col = ['PLACED', 'REGISTERED', 'CONCLUDED', 'CANCELLED']
    order_statuses = status.groupby("order_id").pivot("value", col).agg(first("created_at"))
    order_statuses.show()
    order_statuses.write.parquet(f'trusted/{output_path}')


    # Creating the order items dataset and wrinting to trusted data
    for order_path in order_paths:
        filename = order_path.split('/')[-1]
        output_path = filename.replace('.json.gz', '')

        order = spark.read.parquet(f'file:///home/gerson/raw/order/{output_path}')
        order = order.select('order_id', 'items')
    
        order_items_dataset = order.withColumn('col', from_json("items", ArrayType(StringType())))
        order_items_dataset = order_items_dataset.withColumn('explode_col', explode("col"))
        order_items_dataset = order_items_dataset.withColumn('col', from_json("explode_col", MapType(StringType(), StringType())))
        order_items_dataset = order_items_dataset.withColumn("name", order_items_dataset.col.getItem("name")).withColumn("addition", order_items_dataset.col.getItem("addition")).withColumn("discount", order_items_dataset.col.getItem("discount")).withColumn("quantity", order_items_dataset.col.getItem("quantity")).withColumn("sequence", order_items_dataset.col.getItem("sequence")).withColumn("unitPrice", order_items_dataset.col.getItem("unitPrice")).withColumn("externalId", order_items_dataset.col.getItem("externalId")).withColumn("totalValue", order_items_dataset.col.getItem("totalValue")).withColumn("customerNote", order_items_dataset.col.getItem("customerNote")).withColumn("garnishItems", order_items_dataset.col.getItem("garnishItems")).withColumn("integrationId", order_items_dataset.col.getItem("integrationId")).withColumn("totalAddition", order_items_dataset.col.getItem("totalAddition")).withColumn("totalDiscount", order_items_dataset.col.getItem("totalDiscount"))
        order_items_dataset = order_items_dataset.select('order_id', 'name', 'addition', 'discount', 'quantity', 'sequence', 'unitPrice', 'externalId', 'totalValue', 'customerNote', 'garnishItems', 'integrationId', 'totalAddition', 'totalDiscount')
        order_items_dataset.write.parquet(f'trusted/order_items_dataset/{output_path}')

    # Creating the complete order dataset and wrinting to trusted data
    # First we need to create the last status for every order
    w = Window.partitionBy(status['order_id']).orderBy(status['created_at'].desc())
    status = status.withColumn('Rank', dense_rank().over(w))
    last_status = status.filter(status.Rank == 1).drop(status.Rank)
    # Rename to avoid duplicataes
    last_status = last_status.withColumnRenamed('created_at', 'status_created_at')

    restaurant = spark.read.parquet('file:///home/gerson/raw/restaurant')
 
    consumer = spark.read.parquet('file:///home/gerson/raw/consumer')
    # Rename to avoid duplictaes and drop sensitive columns
    consumer = consumer.withColumnRenamed('created_at', 'cunsumer_created_at').drop('customer_name', 'customer_phone_area', 'customer_phone_number')


    for order_path in order_paths:
        filename = order_path.split('/')[-1]
        output_path = filename.replace('.json.gz', '')
        order = spark.read.parquet(f'file:///home/gerson/raw/order/{output_path}')
        
        # Drop sensitive columns
        order_columns_drop = ['delivery_address_external_id', 'delivery_address_latitude', 'delivery_address_longitude', 'merchant_latitude', 'merchant_longitude']
        order = order.drop(*order_columns_drop)
        # Creating a new column with local time
        order = order.select('*', expr('from_utc_timestamp(order_created_at, merchant_timezone)').alias("timestamp_local"))
        
        order_dataset = order.join(restaurant, order["merchant_id"] == restaurant['id']).join(consumer, ['customer_id']).join(last_status, ["order_id"])
        order_dataset.write.parquet(f'trusted/complete_order_dataset/{output_path}')

    





