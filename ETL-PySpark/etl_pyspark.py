from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import os

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('new-york-taxi-pipeline') \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = create_spark_session()

    #Get the environment variables
    TRIP_DATA_PATH = os.environ.get('TRIP_DATA_PATH', 'gs://new-york-taxi-    data-bucket/trip_data/trip_data_test.csv')
    FARE_DATA_PATH = os.environ.get('FARE_DATA_PATH', 'gs://new-york-taxi-data-bucket/fare_data/fare_data_test.csv')
    BUCKET_NAME = os.environ.get('BUCKET_NAME', 'new-york-taxi-tutorial-project-temporary-bucket')
    BQ_DATASET = os.environ.get('BQ_DATASET', 'new_york_taxi_bq_dataset')
    BQ_TABLE = os.environ.get('BQ_TABLE', 'new_york_taxi_bq_table')
    
    #Extraction phase
    ##read data from cloud storage
    trip_data = spark.read.option("header", True).option("inferSchema", True).csv(TRIP_DATA_PATH)
    fare_data = spark.read.option("header", True).option("inferSchema", True).csv(FARE_DATA_PATH)

    
    #Transformation Phase
    ##remove leading white space from column names
    ###trip data
    old_column_names = [
        ' hack_license',
        ' vendor_id',
        ' rate_code',
        ' store_and_fwd_flag',
        ' pickup_datetime',
        ' dropoff_datetime',
        ' passenger_count',
        ' trip_time_in_secs',
        ' trip_distance',
        ' pickup_longitude',
        ' pickup_latitude',
        ' dropoff_longitude',
        ' dropoff_latitude'
        ]
    
    new_column_names = [
        'hack_license',
        'vendor_id',
        'rate_code',
        'store_and_fwd_flag',
        'pickup_datetime',
        'dropoff_datetime',
        'passenger_count',
        'trip_time_in_secs',
        'trip_distance',
        'pickup_longitude',
        'pickup_latitude',
        'dropoff_longitude',
        'dropoff_latitude'
        ]
    
    for i in range(len(old_column_names)):
        trip_data = trip_data.withColumnRenamed(old_column_names[i], new_column_names[i])

    ###fare data
    old_column_names = [
        ' hack_license',
        ' vendor_id',
        ' pickup_datetime',
        ' payment_type',
        ' fare_amount',
        ' surcharge',
        ' mta_tax',
        ' tip_amount',
        ' tolls_amount',
        ' total_amount'
        ]
    
    new_column_names = [
        'hack_license',
        'vendor_id',
        'pickup_datetime',
        'payment_type',
        'fare_amount',
        'surcharge',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'total_amount'
        ]
    
    for i in range(len(old_column_names)):
        fare_data = fare_data.withColumnRenamed(old_column_names[i], new_column_names[i])

    ###convert string to timestamp
    trip_data = trip_data.withColumn('pickup_datetime', to_timestamp('pickup_datetime')) \
            .withColumn('dropoff_datetime', to_timestamp('dropoff_datetime'))
    

    ##Join Trip Data and Fare Data
    trip_data.createOrReplaceTempView("trip_data")
    fare_data.createOrReplaceTempView("fare_data")

    final_df = spark.sql('''
    select
        a.id, 
        a.medallion,
        a.hack_license,
        a.vendor_id,
        a.rate_code,
        a.pickup_datetime,
        a.dropoff_datetime,
        a.passenger_count,
        a.trip_time_in_secs,
        a.trip_distance,
        a.pickup_longitude,
        a.pickup_latitude,
        a.dropoff_longitude,
        a.dropoff_latitude,
        b.payment_type,
        b.fare_amount,
        b.surcharge,
        b.mta_tax,
        b.tip_amount,
        b.tolls_amount,
        b.total_amount
    from trip_data as a 
        left join fare_data as b
            on a.id = b.id
    '''
                         )
    
    ##data validation and data accuracy
    ###drop null values in 'passenger_count', 'trip_time_in_secs', 'trip_distance', 'fare_amount'
    final_df = final_df.na.drop(subset=[
    'passenger_count',
    'trip_time_in_secs',
    'trip_distance',
    'fare_amount'
    ])

    ###'passenger_count', 'trip_time_in_secs', 'trip_distance', 'fare_amount' must be greater than 0
    final_df = final_df.filter((final_df.passenger_count > 0) & \
                           (final_df.trip_time_in_secs > 0) & \
                           (final_df.trip_distance > 0) & \
                           (final_df.fare_amount > 0)
                          )


    #Loading Phase
    ##Saving the final to BigQuery
    ###Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
    spark.conf.set('temporaryGcsBucket', BUCKET_NAME)

    final_df.write.format('bigquery') \
    .option('table', f'{BQ_DATASET}.{BQ_TABLE}') \
    .mode('append') \
    .save()