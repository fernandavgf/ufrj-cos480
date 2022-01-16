# -*- coding: utf-8 -*-

from pickle import FALSE
import hashFile as hashDB
import orderedFile as orderedDB

csvFilePath = "./input.csv"

#hashDB.createHashBD(csvFilePath)
#orderedDB.createOrderedDB(csvFilePath)

#hashDB.hashSelectRecord(["1230"])
#hashDB.hashDeleteRecord(["1230"])

#for i in range(11):
#    orderedDB.insertLineIntoFile('3999UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n')

# orderedDB.insertMultiple([
    # "3999UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3998UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3997UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3996UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPM#################UNIVERSIDADE PRESBITERIANA MACKENZINHO################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPM#################UNIVERSIDADE PRESBITERIANA MACKENZIO##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPM#################UNIVERSIDADE PRESBITERIANA MACKENZAO##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPM#################UNIVERSIDADE PRESBITERIANA MAHOE######################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPM#################UNIVERSO PRESBITERIANO MACKENZIE######################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999UPP#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n",
    # "3999OBA#################UNIVERSIDADE PRESBITERIANA MACKENZIE##################################################33024014030F5MESTRADO PROFISSIONAL EM ECONOMIA E MERCADOS##################################################################\n"])

#for i in range(3):
#    orderedDB.DeleteSingleOrdered(str(int(i/2+4121)))
#orderedDB.DeleteMultipleOrdered("UPM#################",1)

#orderedDB.OrderedSelectSingleRecord("4245")
#orderedDB.OrderedSelectMultipleRecords(["1000","1234","9321","6543","4545"])
#orderedDB.OrderedSelectInterval(1234,1243)
#orderedDB.OrderedSelectNotUnique("UPM#################",1)