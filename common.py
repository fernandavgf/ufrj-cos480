import csv
import os
import time
import datetime
import fileinput

paddingCharacter = "#"
blockSize = 5
bucketSize = 10
numberOfBuckets = 220
maxColSizesList = [4,20,86,13,110] 
recordSize = sum(maxColSizesList)+1 #233 chars + escape key
heapHeadSize = 5

dicColHeaderType = {
        "COD": "INTEGER(4)",
        'SG_ENTIDADE_ENSINO': "VARCHAR(20)", 
        'NM_ENTIDADE_ENSINO': "VARCHAR(86)", 
        'CD_CURSO_PPG': "VARCHAR(13)", 
        'NM_CURSO': "VARCHAR(110)", 
}

colHeadersList = ["COD", 'SG_ENTIDADE_ENSINO', 'NM_ENTIDADE_ENSINO', "CD_CURSO_PPG", "NM_CURSO"]

def readFromFile(csvFilePath):
    lineCount = 0
    registros = []
    with open(csvFilePath, 'r',  encoding="ISO-8859-1") as file:
        rows = csv.reader(file, delimiter = ";")
        for row in rows:
            if lineCount == 0:
                lineCount+=1
            else:
                finalRow = []
                
                for i in range(len(row)):
                    finalRow += [(row[i])]
                if finalRow[0] == "":
                    return registros
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

def padString(stringToPad, totalSizeOfField, variablePadding=False):
    tmp = stringToPad
    if variablePadding:
        return tmp + paddingCharacter
    for i in range (totalSizeOfField - len(stringToPad)):
        tmp+=paddingCharacter
    return tmp        

def padRecords(listOfRecords, variablePadding=False):
    for i in range(len(listOfRecords)):
        for j in range(len(listOfRecords[i])):
            listOfRecords[i][j] = padString(listOfRecords[i][j], maxColSizesList[j], variablePadding)
    return listOfRecords

def fillCod(cod):
    return cod.zfill(maxColSizesList[0])

def updateHEAD(headPath, headType, numRecords):
    if os.path.exists(headPath):
        file = open(headPath, 'r')
    
        headContent = file.readlines()

        file.close()
        os.remove(headPath)
        
        file = open(headPath, 'a')
        file.write(headContent[0])
        file.write(headContent[1])
        file.write("Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n")
        file.write(headContent[3])
        file.write("Number of records: " + str(numRecords) + "\n")
    else:
        makeHEAD(headPath, headType, numRecords)

def queryHEADrecords(headPath, headSize):
    with open(headPath, 'r') as file:
        for i in range(headSize-1):
            file.readline()
        return (int(file.readline().split("Number of records: ")[1]))

def cleanRecord(recordString):
    newRecord = []
    offset = 0
    for i in range(len(maxColSizesList)):
        newRecord += [recordString[offset:offset+maxColSizesList[i]].replace(paddingCharacter, "").replace("\n", "")]
        
        offset+=maxColSizesList[i]
    return newRecord

def fetchBlock(DBFilePath, startingRecord, variableHeap=False):
    block = []
    with open(DBFilePath, 'r') as file:
        if variableHeap:
            for i in range(startingRecord):
                c = file.readline()
        else:
            for i in range(recordSize*startingRecord):
                c = file.read(1)
      
        if variableHeap:
            for i in range(blockSize):
                record = file.readline()
                if record == "": return block
                record = record.rstrip()
                block += [record.split(paddingCharacter)]
        else:
            for i in range(blockSize):
                record = ""
                for j in range(recordSize):
                    c = file.read(1)

                    if c == "": 
                        return block
                    record+=c

                block += [cleanRecord(record)]
    return block

def deleteLineFromFile(location, filepath):

    for line in fileinput.input(filepath, inplace=1):

        linenum = fileinput.lineno()
        
        if linenum == location+1:
            continue
        else:
            line = line.rstrip()
            print(line)

def markLineDeleted(location, filepath, row = "#" * (recordSize - 1)):

    for line in fileinput.input(filepath, inplace=1):

        linenum = fileinput.lineno()
        
        if linenum == location+1:
            line = row
            print(line)
            continue
        else:
            line = line.rstrip()
            print(line)