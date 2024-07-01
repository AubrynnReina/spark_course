from pyspark import SparkConf, SparkContext


def parse_line_into_age_and_num_friends(line) -> tuple[int, int]:

    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


spark_conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf=spark_conf)

data = sc.textFile('../../data/fakefriends.csv')
age_and_num_friends = data.map(parse_line_into_age_and_num_friends)
total_by_age = age_and_num_friends.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = total_by_age.mapValues(lambda x: round(x[0] / x[1], 2))

results = average_by_age.collect()

for result in results:
    print(result)
