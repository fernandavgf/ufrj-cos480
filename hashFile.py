import common as aux
import os

dbPath = "data.db"
dbHeaderPath = "data.h"


class Record:

    def __init__(self, listOfValues, dataInBytes):
        if (not dataInBytes):
            self.docNumber        = listOfValues[0]
            self.state            = listOfValues[1]
            self.jobType          = listOfValues[2]
            self.candidateNumber  = listOfValues[3]
            self.candidateName    = listOfValues[4]
            self.candidateEmail   = listOfValues[5]
            self.partyNumber      = listOfValues[6]
            self.birthDate        = listOfValues[7]
            self.gender           = listOfValues[8]
            self.instructionLevel = listOfValues[9]
            self.maritalStatus    = listOfValues[10]
            self.colorRace        = listOfValues[11]
            self.ocupation        = listOfValues[12]
        else:
            listOfValues = listOfValues.decode("utf-8")
            self.docNumber        = listOfValues[0:11]
            self.state            = listOfValues[11:13]
            self.jobType          = listOfValues[13:15]
            self.candidateNumber  = listOfValues[15:20]
            self.candidateName    = listOfValues[20:90]
            self.candidateEmail   = listOfValues[90:133]
            self.partyNumber      = listOfValues[133:135]
            self.birthDate        = listOfValues[135:145]
            self.gender           = listOfValues[145:146]
            self.instructionLevel = listOfValues[146:147]
            self.maritalStatus    = listOfValues[147:148]
            self.colorRace        = listOfValues[148:150]
            self.ocupation        = listOfValues[150:153]

        self.sizeInBytes = len(str(self))
    
    def __str__(self):
        return self.docNumber + self.state + self.jobType + self.candidateNumber + self.candidateName + self.candidateEmail + self.partyNumber + self.birthDate + self.gender + self.instructionLevel + self.maritalStatus + self.colorRace + self.ocupation

    def Clear(self):
        self.docNumber        = '\x00' * 11
        self.state            = '\x00' * 2
        self.jobType          = '\x00' * 2
        self.candidateNumber  = '\x00' * 5
        self.candidateName    = '\x00' * 70
        self.candidateEmail   = '\x00' * 43
        self.partyNumber      = '\x00' * 2
        self.birthDate        = '\x00' * 10
        self.gender           = '\x00' * 1
        self.instructionLevel = '\x00' * 1
        self.maritalStatus    = '\x00' * 1
        self.colorRace        = '\x00' * 2
        self.ocupation        = '\x00' * 3

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
                if (self.recordList[i].docNumber.index('\x00') >= 0):
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

def hashInsertRecord(record):
    freeBlockIndex = -1
    freeSpaceIndex = -1

    #calculate hash key and address
    hashKey     = calculateHashKey(record.docNumber)
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
    
    # Create HEAD to File
    aux.makeHEAD(dbHeaderPath, "Hash", 0)
    
    recordCounter = 0
    #inserimos valor a valor com a função de inserção do Hash
    for row in valuesToLoad:
        record = Record(row, False)
        hashInsertRecord(record)
        recordCounter +=1
    print(recordCounter)