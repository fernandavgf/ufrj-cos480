import common as aux
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
        #iterate over records bytes
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

    #Reads the csv file and create the records to be inserted, with fixed length
    valuesToLoad = aux.padRecords(aux.readFromFile(csvFilePath))
    
    # Delete previous database
    if os.path.exists(dbPath):
        os.remove(dbPath)
    
    # Create empty file to reserve disk space
    with open(dbPath, 'wb') as hashFile:
        hashFile.seek((aux.bucketSize * aux.numberOfBuckets * aux.blockSize * (aux.recordSize -1)) - 1)
        hashFile.write(b'\0')
    
   
    recordCounter = 0
    #inserimos valor a valor com a função de inserção do Hash
    for row in valuesToLoad:
        record = Record(row, False)
        hashInsertRecord(record)
        recordCounter +=1

    # Create HEAD to File
    aux.makeHEAD(dbHeaderPath, "Hash", recordCounter)

def hashInsertRecord(record):
    freeBlockIndex = -1
    freeSpaceIndex = -1
    
    #calculate hash key and address
    hashKey     = calculateHashKey(record.cod)
    hashAddress = calculateHashAddress(hashKey)

    # Init the start offset
    startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)

    # Place the record the first block with enough space starting from the file
    with open(dbPath, 'r+b') as hashFile:
        while freeBlockIndex == -1:
            # Load the bucket
            currentBucket = Bucket(hashFile, startingOffset)
            freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
            # Check if there is a collision
            if (freeBlockIndex == -1):
                #If the collision happened a lot and the bucket is full, load the next bucket
                startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                pass
            else:
                # Select block
                currentBlock = currentBucket.blocksList[freeBlockIndex]

                # Set record to rigth block
                freeSpaceIndex = currentBlock.firstEmptyRecordIndex
                currentBlock.recordList[freeSpaceIndex] = record
        
        # Re-write block to the file
        hashFile.seek(startingOffset + (freeBlockIndex * aux.blockSize * (aux.recordSize - 1)))
        hashFile.write(str(currentBlock).encode("utf-8"))

def hashDeleteRecord(searchKeys):
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        #calculate hash key and address
        hashKey     = calculateHashKey(searchKey)
        hashAddress = calculateHashAddress(hashKey)

        # Init the start offset
        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)

        # Place the record the first block with enough space starting from the file
        with open(dbPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                # Load the bucket
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False
                # Search for the key in the registries in the bucket
                for i in range(len(currentBucket.blocksList)):
                    block = currentBucket.blocksList[i]
                    blocksVisitedCount += 1
                    for record in block.recordList:
                        if (record.cod == searchKey):
                            record.Clear()
                            foundRecord = True
                            # Re-write block to the file
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
                    # if record was not found and the bucket is full, it may have occured overflow, so we search in the next bucket
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                        pass
                    # else, print an error and continue
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

def hashSelectRecord(searchKeys):
    recordList = []
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        #calculate hash key and address
        hashKey     = calculateHashKey(searchKey)
        hashAddress = calculateHashAddress(hashKey)
        # Init the start offset
        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.recordSize - 1)

        # Place the record the first block with enough space starting from the file
        with open(dbPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                # Load the bucket
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRecord = False
                # Search for the key in the records in the bucket
                for block in currentBucket.blocksList:
                    blocksVisitedCount += 1
                    for record in block.recordList:
                        if (record.cod == searchKey):
                            print(record)
                            recordList += [record]
                            foundRecord = True
                            print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break
                    
                    if (foundRecord):
                        break

                if (not foundRecord):
                    # if record was not found and the bucket is full, it may have occured overflow, so we search in the next bucket
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.recordSize - 1)
                        pass
                    # else, print an error and continue
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

    return recordList
