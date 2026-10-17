from pyspark.sql import DataFrame, SparkSession 
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType , DoubleType , BooleanType
import pyspark.sql.functions as F
from config import configuration


def main():
    spark = (
        SparkSession
        .builder
        .appName("SmartCityStreaming")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration['AWS_ACCESS_KEY'])
        .config("spark.hadoop.fs.s3a.secret.key", configuration['AWS_SECRET_KEY'])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "20")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.retry.limit", "20")
        .getOrCreate()
        )

    spark.sparkContext.setLogLevel("WARN")


    # Vehicle Schema

    vehicleSchema = StructType([
        StructField("id" , StringType() ,True ),
        StructField("device_id" , StringType() ,True ),
        StructField("timestamp" , TimestampType() ,True ),
        StructField("location" , StringType() ,True ),
        StructField("speed" , DoubleType() ,True ),
        StructField("direction" , StringType() ,True ),
        StructField("make" , StringType() ,True ),
        StructField("model" , StringType() ,True ),
        StructField("year" , IntegerType() ,True ),
        StructField("fuelType" , StringType() ,True ),
    
    ])

    # GPS Schema

    gpsSchema = StructType([
        StructField("id" , StringType() ,True ),
        StructField("device_id" , StringType() ,True ),
        StructField("timestamp" , TimestampType() ,True ),
        StructField("speed" , DoubleType() ,True ),
        StructField("direction" , StringType() ,True ),
        StructField("vehicle_type" , StringType() ,True ),

    ])
    
    # traffic Schema

    trafficSchema = StructType([
        StructField("id" , StringType() ,True ),
        StructField("device_id" , StringType() ,True ),
        StructField("timestamp" , TimestampType() ,True ),
        StructField("camera_id" , StringType() ,True ),
        StructField("location" , StringType() ,True ),
        StructField("snapshot" , StringType() ,True )
    
    ])

    # weather Schema

    weatherSchema = StructType([
        StructField("id" , StringType() ,True ),
        StructField("device_id" , StringType() ,True ),
        StructField("timestamp" , TimestampType() ,True ),
        StructField("location" , StringType() ,True ),
        StructField("temperature" , DoubleType() ,True ),
        StructField("weatherCondition" , StringType() ,True ),
        StructField("feels_like" , DoubleType() ,True ),
        StructField("humidity" , IntegerType() ,True ),
        StructField("pressure" , IntegerType() ,True ),
        StructField("windSpeed" , DoubleType() ,True ),
        StructField("visibility" , DoubleType() ,True ),
        StructField("uv_index" , IntegerType() ,True ),
        StructField("is_day" , BooleanType() ,True ),
    ])


    # emergency Schema

    emergencySchema = StructType([
        StructField("id" , StringType() ,True ),
        StructField("device_id" , StringType() ,True ),
        StructField("timestamp" , TimestampType() ,True ),
        StructField("location" , StringType() ,True ),
        StructField("incidentID" , StringType() ,True ),
        StructField("type" , StringType() ,True ),
        StructField("status" , StringType() ,True )
    ])
    
    def read_kafka_topic(topic, schema):
        return (
            spark
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)              
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(F.from_json(F.col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
        )

    def streamWriter(input:DataFrame , checkPointFolder , output):
        
        return (
            input.writeStream
            .format('parquet')
            .option('checkpointLocation' , checkPointFolder)
            .option('path' , output)
            .outputMode('append')
            .start()
        )
    
    
    vehicleDF = read_kafka_topic('vehicle_data' , vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data' , gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data' , trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data' , weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data' , emergencySchema).alias('emergency')


    query1 = streamWriter(vehicleDF , 's3a://smart-city-streaming-data-458195084286/checkpoints/vehicle_data' , 's3a://smart-city-streaming-data-458195084286/data/vehicle_data')
    query2 = streamWriter(gpsDF , 's3a://smart-city-streaming-data-458195084286/checkpoints/gps_data' , 's3a://smart-city-streaming-data-458195084286/data/gps_data')
    query3 = streamWriter(trafficDF , 's3a://smart-city-streaming-data-458195084286/checkpoints/traffic_data' , 's3a://smart-city-streaming-data-458195084286/data/traffic_data')
    query4 = streamWriter(weatherDF , 's3a://smart-city-streaming-data-458195084286/checkpoints/weather_data' , 's3a://smart-city-streaming-data-458195084286/data/weather_data')
    query5 = streamWriter(emergencyDF , 's3a://smart-city-streaming-data-458195084286/checkpoints/emergency_data' , 's3a://smart-city-streaming-data-458195084286/data/emergency_data')

    query5.awaitTermination()


if __name__ == "__main__":
    main()