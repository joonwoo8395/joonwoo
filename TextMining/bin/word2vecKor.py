# -*- coding: utf-8 -*-
import gensim
import os
import sys
import io

class Word2VecModule():
    def __init__(self):
        pass

    def run(self, line):
        model = gensim.models.Word2Vec.load('ko.bin')
        module = os.path.basename(sys.argv[0])
        
        listB = line.split(",")
        resList = []
        for x in listB[:-1]:
            print (x)
            y = x.rstrip()
            try:
                # resDict[oneWord] = model.wv.most_similar(positive=[oneWord])
                t = model.wv.most_similar(y)
            except KeyError as e:
                print('%s is not included Dictionary' % y)
                t = '%s is not included Dictionary' % y
            except Exception as ex:
                # print(ex)
                t = ex
            resList.append(t)

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
