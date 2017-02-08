from pyspark import SparkConf,SparkContext
from nltk.tokenize import RegexpTokenizer
from itertools import chain


def main():


    #Setting up the standalone mode
    conf = SparkConf().setMaster("local").setAppName("Preprocess")
    sc = SparkContext(conf = conf)

    # Initialize the tokenizer
    tokenizer = RegexpTokenizer(r'\w+')

    # Load in RDD and RDD persist for multiple operation
    lines = sc.textFile("./tweets_smaller.txt");


    # Map convert each json to array object rdd and plain text

    #Rdd transformation 
    js = lines.flatMap(lambda x : x.split('\n')).map(lambda line : tokenizer.tokenize(line)).map(lambda x : " ".join(x) + ".");
    rs = js.take(1);
    print rs;


if __name__ == "__main__": main()
