import nltk
nltk.download('movie_reviews')
import os
import sys
import io
from nltk.corpus import movie_reviews
sentences = [list(s) for s in movie_reviews.sents()]
from gensim.models.word2vec import Word2Vec
model = Word2Vec(sentences)

class Word2VecModule():
    def __init__(self):
        pass

    def run(self, line):

        module = os.path.basename(sys.argv[0])

        listB = line.split(",")
        resList = []
        for x in listB[:-1]:
            print (x)
            y = x.rstrip()
            resList.append(model.most_similar(y))

        print(resList)
        inputString = sys.argv[1]

        filename = '../workd2vecFile/res_%s' % inputString
        fout = open(filename, 'w')
        for t in enumerate(resList):
            a = 'index : {} value: {}'.format(*t)
            print(a)
            fout.write(a)

def main():
    f = open(sys.argv[1], "r")
    line = f.readline()
    wv = Word2VecModule()
    wv.run(line)

if __name__ == '__main__':
    try:
        main()
    except ValueError:
        print(ValueError)

