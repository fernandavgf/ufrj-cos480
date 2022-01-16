import csv
import os
import time
import datetime


paddingCharacter = "#"
blockSize = 5
bucketSize = 10            #Tamanho do bucket do hash (em blocos)
numberOfBuckets = 220      #Quantidade máxima de buckets
recordSize = 233+1         #153 chars + escape key


dicColHeaderType = {
        "COD": "INTEGER(4)",
        'SG_ENTIDADE_ENSINO': "VARCHAR(20)", 
        'NM_ENTIDADE_ENSINO': "VARCHAR(86)", 
        'CD_CURSO_PPG': "VARCHAR(13)", 
        'NM_CURSO': "VARCHAR(110)", 
}

#Baseado no dic acima(CPF JOGADO PARA A PRIMEIRA POSICAO)
maxColSizesList = [4,20,86,13,110]

#Baseado no dic acima(e na ordem da lista acima, com CPF no início)
colHeadersList = ["COD", 'SG_ENTIDADE_ENSINO', 'NM_ENTIDADE_ENSINO', "CD_CURSO_PPG", "NM_CURSO"]


def readFromFile(csvFilePath):
    lineCount = 0
    registros = []
    with open(csvFilePath, 'r',  encoding="ISO-8859-1") as file:
        rows = csv.reader(file, delimiter = ";")
        for row in rows:
            if lineCount == 0 :#headers
                lineCount+=1
            else:
                finalRow = []
                
                for i in range(len(row)):
                    finalRow += [(row[i])]
                if finalRow[0] == "":
                    return registros      #chegou numa linha vazia, fim do arquivo
                registros +=[finalRow]
                lineCount+=1
    return registros

def makeHEADString(headType, numRecords):
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "\nNumber of records: " + str(numRecords) + "\n"
    
    return string

def makeHEAD(headPath, headType, numRecords):
    if os.path.exists(headPath):
        os.remove(headPath)
    file = open(headPath, 'a')
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "\nNumber of records: " + str(numRecords) + "\n"
    file.write(string)


def padString(stringToPad, totalSizeOfField):
    tmp = stringToPad
    for i in range (totalSizeOfField - len(stringToPad)):
        tmp+=paddingCharacter
    return tmp        

def padRecords(listOfRecords):
    for i in range(len(listOfRecords)):
        for j in range(len(listOfRecords[i])):
            listOfRecords[i][j] = padString(listOfRecords[i][j], maxColSizesList[j])
    return listOfRecords