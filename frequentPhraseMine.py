
import sys
from pyspark import SparkConf,SparkContext
from nltk.tokenize import RegexpTokenizer
from itertools import chain
import json
from nltk.tokenize import word_tokenize



tokenizer = RegexpTokenizer(r'\w+')
#tokenizer = RegexpTokenizer('[A-Z]\w+')

class Utility:
    @staticmethod
    def ngrams(input, index, n,phasefrequency,frequecy,minFrequency):
        #Parse the input
        input = input.split(' ')
        output = []

        #Special case
        if(n == 1):
            for i in range(len(input)):
                output.append(i) # append index
            return {index:output}

        for subindex in phasefrequency.get(index):
            # range(x,y) -> x,y-1
            # Make sure it doesn't exceed the range
            if(subindex + n - 2 >= len(input)):
                continue;

            string = " ".join([ input[i] for i in range(subindex, subindex + n - 1)]);
            if frequecy.has_key(string) and frequecy.get(string) >= minFrequency:
                output.append(subindex) #append index
        return {index:output}

    @staticmethod
    def mergeDict(dict1, dict2):
        return dict(dict1.items()+ dict2.items())


    @staticmethod
    def countPhase(input, index, findDict,n):
        input = input.split(' ')
        dict = {}
        if(len(input) == 0):
            return dict

        # document -> indices
        list = findDict[index]

        #Expand n + 1 grams
        for subindex in list:
            if(subindex + n - 1 >= len(input)): continue;
            # x to x+n-1
            string = " ".join([input[i] for i in range(subindex,subindex + n)]);
            if dict.has_key(string):
                dict[string] += 1
            else:
                dict[string] = 1

        return dict

    @staticmethod
    def mergeFrequency(dict1, dict2):
        mergeDict = {}
        for (k,v) in dict1.iteritems():
            mergeDict[k] = dict1.get(k)


        for (k,v) in dict2.iteritems():
            if(mergeDict.has_key(k)):
                mergeDict[k] += dict2.get(k)
            else:
                mergeDict[k] = dict2.get(k);

        return mergeDict

    @staticmethod
    def loadJson(line):
        formatStr = ""
        tweet = json.loads(line).get('tweet')
        if (tweet.get('text') != None and tweet.get('lang') == 'en'):
            formatStr = " ".join(tokenizer.tokenize(tweet.get('text'))) + ".";
        return formatStr;






# Driver Program
def frequentMine(Filepath, iteration = 10, minimumSupport = 6, tweets = False, verbose = False ):

    minFrequency = minimumSupport;
    #Setting up the standalone mode
    conf = SparkConf().setMaster("local").setAppName("LogisticClassifer")
    sc = SparkContext(conf = conf)

    # Load in RDD and RDD persist for multiple operation
    lines = sc.textFile(Filepath)

    if tweets:
        lines = lines.flatMap(lambda x : x.split('\n')) \
            .map(lambda line : Utility.loadJson(line))


    #Serires of operations										# Get rid of len 0 line 			#get zipWithIndex
    idxSentence = lines.flatMap(lambda line : line.split(".")) \
                        .filter(lambda line : len(line) > 0 ).zipWithIndex().persist()

    #Set up datastructure
    phasefrequency = {}
    frequecy = {}
    result = {}

    # Main Logic of Phase Mining
    # i is the i-grams, default to 8
    for i in range(1,iteration):
        #First Map reduce job get the all N-gram that satisfy minimum support
        #And put them with corresponding index
        #Map should take one argument (x,y)
        nGram = idxSentence \
            .map( lambda (x,y) : Utility.ngrams(x,y,i,phasefrequency,frequecy,minFrequency))

        if verbose:
            print "debug"
            print nGram.collect();

        #Merge all possible #document -> index of ngram
        findDict = nGram \
            .reduce( lambda x,y : Utility.mergeDict(x,y))

        #Pruning
        idxSentence = idxSentence \
            .filter( lambda (x,y): findDict.has_key(y) \
                and findDict[y] != None \
                and len(findDict[y]) > 0 )

        if verbose:
            print "The gram", i
            print "idxSentence",idxSentence.collect()
            print "The findDict",findDict
        #Second Map reduce job to Count frequecy
        frequencyDict = idxSentence \
            .map(lambda (x,y) : Utility.countPhase(x,y,findDict,i));
        #Check Empty
        if frequencyDict.isEmpty():
            break
            
        MergeFrequency = frequencyDict \
            .reduce(lambda x,y : Utility.mergeFrequency(x,y))

        filterMergeFrequency = MergeFrequency;

        #Parallize the result out
        ls = sc.parallelize(filterMergeFrequency.items()) \
            .filter(lambda (x,y) : y >= minFrequency and len(x) > 0) \
                .collect();
        result.update(ls);

        if verbose:
            print "The merged frequency", MergeFrequency;
            print "\n\n"
        #Another iteration
        frequecy = MergeFrequency
        phasefrequency = findDict

    #Last process
    print result



def main():
    frequentMine(Filepath ="./tweets_smaller.txt", iteration = 8, minimumSupport = 2, tweets =True,verbose=True)


if __name__ == "__main__": main()
