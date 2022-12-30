"""from https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-application.html """

import argparse
import logging
from operator import add
from random import random
import boto3
from pyspark.sql import SparkSession
import os
import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

session = boto3.Session()

credentials = session.get_credentials()
AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key

s3 = boto3.resource('s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)



def calculate_pi(partitions): #, output_uri):
    """
    Calculates pi by testing a large number of random numbers against a unit circle
    inscribed inside a square. The trials are partitioned so they can be run in
    parallel on cluster instances.

    :param partitions: The number of partitions to use for the calculation.
    :param output_uri: The URI where the output is written, typically an Amazon S3
                       bucket, such as 's3://example-bucket/pi-calc'.
    """
    def calculate_hit(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

    tries = 4000000 * partitions
    logger.info(
        "Calculating pi with a total of %s tries in %s partitions.", tries,
        partitions)
    with SparkSession.builder.appName("My PyPi").getOrCreate() as spark:
        hits = spark.sparkContext.parallelize(range(tries), partitions)\
            .map(calculate_hit)\
            .reduce(add)
        pi = 4.0 * hits / tries
        logger.info(
        "****** %s tries and %s hits gives pi estimate of %s. Pi= %s ******",
        tries, hits, pi, np.pi)
        #if output_uri is not None:
        df = spark.createDataFrame(
            [(tries, hits, pi)], ['tries', 'hits', 'pi'])
        df.write.option("header",True).csv("/Users/josephcerniglia/datacsv/")
        #df.write.mode('overwrite').json(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--partitions', default=2, type=int,
        help="The number of parallel partitions to use when calculating pi.")
    parser.add_argument(
        '--output_uri',
        help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    calculate_pi(args.partitions) #, args.output_uri)
    print('args.output_uri = ',args.output_uri)
    print('args.partitions = ',args.partitions)

#BUCKET=s3.Bucket('pi-bucket55')
bucket_name='pi-bucket55'

def uploadDirectory(path,bucketname):
    #print('in the function')
    try:
        for root,dirs,files in os.walk(path):
            #print(dirs)
            filecount=int(len(files)/2)
            #print(filecount)
            for index_number in range(filecount):
                print('uploading...',files[index_number])
                s3.Bucket('pi-bucket55').upload_file(
                os.path.join(root,files[index_number]),
                "pi-calc/" + files[index_number])
                if index_number == filecount-1:
                        return "Success!"


    except Exception as e:
        print(e)

success=uploadDirectory('/Users/josephcerniglia/datacsv/',bucket_name)
print(success)

# the line below works for one file!
#s3.Bucket('pi-bucket55').upload_file("/Users/josephcerniglia/datacsv/_SUCCESS", "pi-calc/_SUCCESS")
