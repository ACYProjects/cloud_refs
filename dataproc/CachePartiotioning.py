import os
from pyspark import SparkContext

def create_spark_context():
    return SparkContext(appName="RDDExample")

def process_data(sc):
    text_file = f"gs://{BUCKET}/text_file.txt"

    lines = sc.textFile(text_file)

    words = lines.flatMap(lambda line: line.split(" "))

    words_cached = words.cache()

    word_counts = words_cached.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    word_counts.persist()

    word_counts_repartitioned = word_counts.repartition(10)

    top_words = word_counts_repartitioned.takeOrdered(10, key=lambda x: -x[1])

    print(top_words)

if __name__ == "__main__":
    PROJECT_ID = os.environ.get('PROJECT_ID')
    REGION = os.environ.get('REGION')
    BUCKET = os.environ.get('BUCKET')

    sc = create_spark_context()
    process_data(sc)
    sc.stop()
