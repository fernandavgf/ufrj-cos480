import csv
import os
import time
import datetime
import fileinput


paddingCharacter = "#"
blockSize = 5
bucketSize = 10            #Tamanho do bucket do hash (em blocos)
numberOfBuckets = 220      #Quantidade máxima de buckets
recordSize = 233+1         #153 chars + escape key
heapHeadSize = 5           #Tamanho do head do heap(em linhas)


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
    file.close()

def updateHEAD(headPath, headType, numRecords):
    if os.path.exists(headPath):
        file = open(headPath, 'r')
    
        headContent = file.readlines()
        file.close()
        os.remove(headPath)
        
        #recria ela com as alteracoes
        file = open(headPath, 'a')
        file.write(headContent[0])
        file.write(headContent[1])
        file.write("Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n")
        file.write(headContent[3])
        file.write("Number of records: " + str(numRecords) + "\n")
    else:
        #Doesn't exist, create it
        makeHEAD(headPath, headType, numRecords)
    file.close()

def queryHEADrecords(headPath, headSize):
    #posição de início de leitura dos dados
    #cursorBegin = startingR
    with open(headPath, 'r') as file:
        for i in range(headSize-1):
            file.readline()
        return (int(file.readline().split("Number of records: ")[1]))

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

def cleanRecord(recordString):
    newRegistry = []
    offset = 0
    for i in range(len(maxColSizesList)):
        newRegistry += [recordString[offset:offset+maxColSizesList[i]].replace(paddingCharacter, "").replace("\n", "")]
        
        offset+=maxColSizesList[i]
    return newRegistry

def deleteLineFromFile(location, filepath):
    # Open the file
    for line in fileinput.input(filepath, inplace=1):
        # Check line number
        linenum = fileinput.lineno()
        # If we are in our desired location, append the new record to the current one. Else, just remove the line-ending character
        if linenum == location+1:
            continue
        else:
            line = line.rstrip()
            # write line in the output file
            print(line)

def fetchBlock(filePath, startingRecord, fetchRerecordSizecordSize = recordSize):
    block = []
    with open(filePath, 'r', encoding="utf-8") as file:
        for i in range((recordSize - 1)*startingRecord):
            c = file.read(1) #vamos de 1 em 1 char para não jogar tudo de uma vez na memória
        
        for i in range(blockSize):
            record = ""
            for j in range(recordSize - 1):
                c = file.read(1)
                if c == "": 
                    return block
                record+=c
#            print(record)
            block += [cleanRecord(record)]
    return block
