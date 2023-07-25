#################################################################################################
#################################################################################################
#################################################################################################

## write all the functions here, we can connect them all in main.py

#################################################################################################
#################################################################################################
#################################################################################################


import dask
import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client, progress

def get_ngrams_dask(list_of_sentences):

    """
    Given a list of sentences, returns the count dictionary of unigrams, bigrams and trigrams
    """
    
    words = list_of_sentences.map(lambda x: x.split())

    unigram = words.map(lambda x: x)
    bigram = words.map(lambda x: [x[i] + " " + x[i+1] for i in range(len(x)-1)])
    trigram = words.map(lambda x: [x[i] + " " + x[i+1] + " " + x[i+2] for i in range(len(x)-2)])    

    ## ! column names to be noted

    unigram = unigram.flatten().frequencies().to_dataframe(columns=['unigram', 'count'])
    bigram = bigram.flatten().frequencies().to_dataframe(columns=['bigram', 'count'])
    trigram = trigram.flatten().frequencies().to_dataframe(columns=['trigram', 'count'])

def get_ngrams_csv(file_path):
    
    """
    Given a csv file path, saves unigram, bigram and trigram data in csv files
    """

    ## ! must test before using

    client = Client('localhost:8786')

    data = dd.read_csv(file_path, header=None, names=['text'])
    data = data['text']

    unigram, bigram, trigram = get_ngrams_dask(data)

    unigram = unigram.compute()
    bigram = bigram.compute()
    trigram = trigram.compute()

    ## ! path name not specified
    unigram.to_csv('unigram.csv')
    bigram.to_csv('bigram.csv')
    trigram.to_csv('trigram.csv')