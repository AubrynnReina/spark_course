from pyspark import SparkConf, SparkContext


def parse_data(line) -> tuple[str, str, float]:

    # Get station ID, entry type and temperature from a row
    fields = line.split(',')
    station_ID = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3])

    return (station_ID, entry_type, temperature)


conf = SparkConf().setMaster('local').setAppName('Get min temperatures')
sc = SparkContext(conf=conf)

data = sc.textFile('./data/1800.csv')
station_entry_temperature = data.map(parse_data)
entries_with_min_temp = station_entry_temperature.filter(lambda x: x[1] == 'TMIN')
station_temperature = entries_with_min_temp.map(lambda x: (x[0], x[2]))
min_temp_per_station = station_temperature.reduceByKey(lambda x, y: min(x, y))
results = min_temp_per_station.collect()

for result in results:
    print(result)