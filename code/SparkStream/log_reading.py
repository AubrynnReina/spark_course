import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import regexp_extract


spark_session = SparkSession.builder \
                            .appName('Log_Reader') \
                            .getOrCreate()


data = spark_session.readStream.text('data/logs')

content_size_expression = r'\s(\d+)$'
status_expression = r'\s(\d{3})\s'
general_expression = r'\"(\S+)\s(\S+)\s*(\S*)\"'
time_expression = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})]'
host_expresison = r'(^\S+\.[\S+\.]+\S+)\s'


extracted_data = data.select(
    regexp_extract('value', host_expresison, 1).alias('host'),
    regexp_extract('value', time_expression, 1).alias('timestamp'),
    regexp_extract('value', general_expression, 1).alias('method'),
    regexp_extract('value', general_expression, 2).alias('endpoint'),
    regexp_extract('value', general_expression, 3).alias('protocol'),
    regexp_extract('value', status_expression, 1).cast('integer').alias('status'),
    regexp_extract('value', content_size_expression, 1).cast('integer').alias('content_size')
)

status_counts_df = extracted_data.groupBy(extracted_data.status).count()
query = status_counts_df.writeStream \
                        .outputMode('complete') \
                        .format('console') \
                        .queryName('counts') \
                        .start()

query.awaitTermination()
spark_session.stop()