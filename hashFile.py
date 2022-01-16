import common as aux
import codecs
import os

dbPath = "data.db"
dbHeaderPath = "data.h"

class Record:

    def __init__(self, listOfValues, dataInBytes):
        if (not dataInBytes):
            self.cod                                 = listOfValues[0]
            self.sgEntidadeEnsino                    = listOfValues[1]
            self.nmEntidadeEnsino                    = listOfValues[2]
            self.cdCursoPpg                          = listOfValues[3]
            self.nmCurso                             = listOfValues[4]
        else:
            listOfValues = listOfValues.decode("utf-8")
            self.cod                                 = listOfValues[0:4]
            self.sgEntidadeEnsino                    = listOfValues[4:24]
            self.nmEntidadeEnsino                    = listOfValues[24:110]
            self.cdCursoPpg                          = listOfValues[110:123]
            self.nmCurso                             = listOfValues[123:233]

        self.sizeInBytes = len(str(self))
    
    def __str__(self):
        return self.cod + self.sgEntidadeEnsino + self.nmEntidadeEnsino + self.cdCursoPpg + self.nmCurso

    def Clear(self):
        self.cod                 = '\x00' * 4
        self.sgEntidadeEnsino    = '\x00' * 20
        self.nmEntidadeEnsino    = '\x00' * 86
        self.cdCursoPpg          = '\x00' * 13
        self.nmCurso             = '\x00' * 110

        self.sizeInBytes = len(str(self))
class Block:

    def __init__(self, recordBytes):
        self.recordList = []
        for b in range(0, len(recordBytes), (aux.recordSize -1)):
            self.recordList += [Record(recordBytes[b : b + (aux.recordSize -1)], True)]

        self.firstEmptyRecordIndex = self.__FirstEmptyRecordIndex()

    def SizeInBytes(self):
        sizeInBytes = 0
        for record in self.recordList:
            sizeInBytes += record.sizeInBytes

        return sizeInBytes

    def __FirstEmptyRecordIndex(self):
        for i in range(len(self.recordList)):
            try:
                if (self.recordList[i].cod.index('\x00') >= 0):
                    return i
            except:
                pass
        return -1

    def __str__(self):
        str_block = ""
        for record in self.recordList:
            str_block += str(record)
        
        return str_block
class Bucket:
    def __init__(self, hashFile, startOffset):
        self.blocksList = []
        for i in range(startOffset, startOffset + aux.bucketSize * aux.blockSize * (aux.recordSize -1) - 1, aux.blockSize * (aux.recordSize -1)):
            self.blocksList += [Block(fetchBlockBytes(hashFile, i))]
        self.firstBlockWithEmptyRecordIndex = self.__FirstBlockWithEmptyRecordIndex()

    def __FirstBlockWithEmptyRecordIndex(self):
        for i in range(len(self.blocksList)):
            if (self.blocksList[i].firstEmptyRecordIndex != -1):
                return i
        
        return -1

def calculateHashKey(key):
    return int(key)

def calculateHashAddress(hashKey):
    return hashKey % aux.numberOfBuckets

def fetchBlockBytes(hashFile, startOffset):
    hashFile.seek(startOffset)
    return hashFile.read((aux.blockSize * (aux.recordSize -1)))

def createHashBD(csvFilePath):

    valuesToLoad = aux.padRecords(aux.readFromFile(csvFilePath))

    if os.path.exists(dbPath):
        os.remove(dbPath)
    
    with open(dbPath, 'wb') as hashFile:
        hashFile.seek((aux.bucketSize * aux.numberOfBuckets * aux.blockSize * (aux.recordSize -1)) - 1)
        hashFile.write(b'\0')
    recordCounter = 0
    aux.makeHEAD(dbHeaderPath, "Hash", 0)

    for row in valuesToLoad:
        recordCounter += 1
        record = Record(row, False)
        hashInsertRecordSingle(record)
    aux.updateHEAD(dbHeaderPath, "Hash", recordCounter)

def hashInsertRecordSingle(record):
    freeBlockIndex = -1
    freeSpaceIndex = -1
    
    hashKey     = calculateHashKey(record.cod)
    hashAddress = calculateHashAddress(hashKey)

    startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)

    with open(dbPath, 'r+b') as hashFile:
        while freeBlockIndex == -1:
            currentBucket = Bucket(hashFile, startingOffset)
            freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex

            if (freeBlockIndex == -1):
                startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                pass
            else:
                currentBlock = currentBucket.blocksList[freeBlockIndex]
                freeSpaceIndex = currentBlock.firstEmptyRecordIndex
                currentBlock.recordList[freeSpaceIndex] = record

        hashFile.seek(startingOffset + (freeBlockIndex * aux.blockSize * (aux.recordSize - 1)))
        hashFile.write(str(currentBlock).encode("utf-8"))

def hashDeleteRecordById(searchKeys):
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0

        hashKey     = calculateHashKey(searchKey)
        hashAddress = calculateHashAddress(hashKey)

        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)

        with open(dbPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False

                for i in range(len(currentBucket.blocksList)):
                    block = currentBucket.blocksList[i]
                    blocksVisitedCount += 1
                    for record in block.recordList:
                        if (record.cod == searchKey):
                            record.Clear()
                            foundRecord = True
                            hashFile.seek(startingOffset + (i * aux.blockSize * (aux.recordSize - 1)))
                            hashFile.write(str(block).encode("utf-8"))
                            print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                            print("Record deleted")
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break
                    
                    if (foundRecord):
                        break

                if (not foundRecord):
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                        pass
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

def hashSelectId(searchKeys):
    recordList = []
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0

        hashKey     = calculateHashKey(searchKey)
        hashAddress = calculateHashAddress(hashKey)

        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
        print("\nRunning query: ")
        print("\nSELECT * FROM TABLE WHERE COD in " + str(searchKeys) + ";\n\n")

        with open(dbPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False

                for block in currentBucket.blocksList:
                    blocksVisitedCount += 1
                    for record in block.recordList:
                        if (record.cod == searchKey):
                            recordList += [record]
                            foundRecord = True
                            print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break
                    
                    if (foundRecord):
                        print(record)
                        break

                if (not foundRecord):
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                        pass
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

    return recordList

def hashSelectColumns(colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = "", idResult = []):
    blocksScanned = 0
    recordFound = False
    endOfFile = False
    values = ""
    if valueIsArray:
        for val in value:
            values+= val + ", "
        values = values[:len(values)-2]
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName)
    secondValuePresent = False

    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in aux.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = aux.colHeadersList.index(secondColName)
        secondValuePresent = True

    print("\nRunning query: ")
    if singleRecordSelection:
        if valueIsArray:
            print("\nSELECT * FROM TABLE WHERE " + colName + " IN (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TABLE WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TABLE WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nSELECT * FROM TABLE WHERE " + colName + " IN (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TABLE WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TABLE WHERE " + colName + " = " + value + ";\n\n")

    customRecord= 0
    results = []
    while not (recordFound or endOfFile):
        currentBlock = aux.fetchBlock(dbPath, customRecord)
        if currentBlock == []:
            endOfFile = True
            break
        blocksScanned +=1
        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
                results += [currentBlock[i]]
                idResult += [currentBlock[i][0]]
                if singleRecordSelection:
                    recordFound = True
                    break
        customRecord +=aux.blockSize
        
    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print("Results found: \n")
        for result in results:
            print(result)

    print("\n")
    print("End of search.")
    print("Number of blocks fetched: " + str(blocksScanned))
    return idResult

def hashSelectRecord(input1, input2 = "SINGLE", singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = "", betweenTwoValues = False):
    if(str(input2) == "SINGLE"):
        if(betweenTwoValues == False):
            hashSelectId(input1)
        else:
            for i in range(int(input1[0]), int(input1[1])):
                values = []
                values.append(str(i))
                hashSelectId(values)
    else:
        hashSelectColumns(input1, input2, singleRecordSelection, valueIsArray, secondColName, secondValue)

def hashInsertRecord(records):
    records = aux.padRecords(records)
    for i in range(len(records)):
        valueToInsert = Record(records[i], False)
        hashInsertRecordSingle(valueToInsert)

def hashDeleteRecord(input1, input2 = "SINGLE"):
    if(input2 == "SINGLE"):
        hashDeleteRecordById(input1)
    else:
        listToDelete = hashSelectColumns(input1, input2)
        hashDeleteRecord(listToDelete)
