from pyspark import SparkConf, SparkContext


# A function to only get words in lowercase and without punctuations
def get_word_only_from_text(text: str) -> list[str]:
    import re
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster('local').setAppName('Count words')
sc = SparkContext(conf=conf)

data = sc.textFile('../../data/Book')
words = data.flatMap(get_word_only_from_text)

# words_appearances = words.countByValue()
words_appearances = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# These 2 are basically similar, different output tho: 1st is rdd, 2nd is collection.

sorted_words_appearances = words_appearances.sortBy(keyfunc=lambda x: x[1])


for word, count in sorted_words_appearances.collect():

    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(f'{cleanWord.decode()}: {count}')