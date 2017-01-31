
import sys
from pyspark import SparkConf,SparkContext

minFrequency = 6;


class Utility:
    @staticmethod
    def ngrams(input, index, n,phasefrequency,frequecy):
        #Parse the input
        input = input.split(' ')
        output = []

        #Special case
        if(n == 1):
            for i in range(len(input)):
                output.append(i) # append index
            return {index:output}

        for subindex in phasefrequency.get(index):
            string = ""
            for i in range(subindex, subindex + n - 1):
                if (i + n - 2) < len(input):
                    if i == subindex + n - 2:
                        string += input[i]
                    else:
                        string += (input[i] + ' ')

            if frequecy.has_key(string) and frequecy.get(string) > minFrequency:
                output.append(i) #append index
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

        for x in list:
            string = ""
            for i in range(x,x+n):
                if (i + n - 1) < len(input):
                    if i == x + n - 1:
                        string += input[i]
                    else:
                        string += (input[i] + ' ')
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

# Driver Program
def main():


    #Setting up the standalone mode
    conf = SparkConf().setMaster("local").setAppName("LogisticClassifer")
    sc = SparkContext(conf = conf)

    # Load in RDD and RDD persist for multiple operation
    lines = sc.textFile("./random.txt");

    #Serires of operations										# Get rid of len 0 line 			#get zipWithIndex
    idxSentence = lines.flatMap(lambda line : line.split(".")).filter(lambda line : len(line) > 0 ).zipWithIndex().persist()

    phasefrequency = {}
    frequecy = {}
    result = {}

    # Main Logic of Phase Mining
    # i is the i-grams, default to 8
    for i in range(1,8):
        #First Map reduce job get the all N-gram that satisfy minimum support
        #And put them with corresponding index
        #Map should take one argument (x,y)
        nGram = idxSentence.map( lambda (x,y) : Utility.ngrams(x,y,i,phasefrequency,frequecy))

        #Merge all possible #document -> index of ngram
        findDict = nGram.reduce( lambda x,y : Utility.mergeDict(x,y))

        #Pruning
        idxSentence = idxSentence.filter(lambda (x,y): findDict.has_key(y) and findDict[y] != None and len(findDict[y]) > 0 )
        #print "idxSentence"
        #print idxSentence.collect()
        #print "The findDict"
        #print findDict
        #Second Map reduce job to Count frequecy
        frequencyDict = idxSentence.map(lambda (x,y) : Utility.countPhase(x,y,findDict,i));
        #Check Empty
        if frequencyDict.isEmpty():
            break
        MergeFrequency = frequencyDict.reduce(lambda x,y : Utility.mergeFrequency(x,y))
        #print "The merged frequency are"
        #print MergeFrequency
        result.update(MergeFrequency)
        frequecy = MergeFrequency
        phasefrequency = findDict


    print "final frequency is"
    lastRun = []
    for k,v in result.iteritems():
        if(v < minFrequency):
            lastRun.append(k)

    [result.pop(key) for key in lastRun]
    print result


    # Next Step is Phase Construction Algorithm



if __name__ == "__main__": main()
