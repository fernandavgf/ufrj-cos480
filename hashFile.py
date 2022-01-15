import common as aux
import os

dbPath = "data.db"
dbHeaderPath = "data.h"


class Record:

    def __init__(self, listOfValues, dataInBytes):
        if (not dataInBytes):
            self.anBase                              = listOfValues[0]
            self.nmGrandeAreaConhecimento            = listOfValues[1]
            self.nmAreaConhecimento                  = listOfValues[2]
            self.nmSubAreaConhecimento               = listOfValues[3]
            self.nmEspecialidade                     = listOfValues[4]
            self.cdAreaAvaliacao                     = listOfValues[5]

            self.nmAreaAvaliacao                     = listOfValues[6]
            self.cdEntidadeCapes                     = listOfValues[7]
            self.cdEntidadeEmec                      = listOfValues[8]
            self.sgEntidadeEnsino                    = listOfValues[9]
            self.nmEntidadeEnsino                    = listOfValues[10]
            self.csStatusJuridico                    = listOfValues[11]

            self.dsDependenciaAdministrativa         = listOfValues[12]
            self.dsOrganizacaoAcademica              = listOfValues[13]
            self.nmRegiao                            = listOfValues[14]
            self.sgUfPrograma                        = listOfValues[15]
            self.nmMunicipioProgramaIes              = listOfValues[16]
            self.cdProgramaIes                       = listOfValues[17]

            self.nmProgramaIes                       = listOfValues[18]
            self.cdCursoPpg                          = listOfValues[19]
            self.nmCurso                             = listOfValues[20]
            self.nmGrau                              = listOfValues[21]
            self.cdConceitoCurso                     = listOfValues[22]
            self.anInicioPrevisto                    = listOfValues[23]
            
            self.dsSituacaoCurso                     = listOfValues[24]
            self.dtSituacaoCurso                     = listOfValues[25]
            self.idAddFotoProgramaIes                = listOfValues[26]
            self.idAddFotoPrograma                   = listOfValues[27]
        else:
            listOfValues = listOfValues.decode("utf-8")
            self.anBase                              = listOfValues[0:4]
            self.nmGrandeAreaConhecimento            = listOfValues[4:31]
            self.nmAreaConhecimento                  = listOfValues[31:73]
            self.nmSubAreaConhecimento               = listOfValues[73:132]
            self.nmEspecialidade                     = listOfValues[132:189]
            self.cdAreaAvaliacao                     = listOfValues[189:191]

            self.nmAreaAvaliacao                     = listOfValues[191:256]
            self.cdEntidadeCapes                     = listOfValues[256:264]
            self.cdEntidadeEmec                      = listOfValues[264:269]
            self.sgEntidadeEnsino                    = listOfValues[269:289]
            self.nmEntidadeEnsino                    = listOfValues[289:375]
            self.csStatusJuridico                    = listOfValues[375:385]

            self.dsDependenciaAdministrativa         = listOfValues[385:392]
            self.dsOrganizacaoAcademica              = listOfValues[392:443]
            self.nmRegiao                            = listOfValues[443:455]
            self.sgUfPrograma                        = listOfValues[455:457]
            self.nmMunicipioProgramaIes              = listOfValues[457:482]
            self.cdProgramaIes                       = listOfValues[482:495]

            self.nmProgramaIes                       = listOfValues[495:599]
            self.cdCursoPpg                          = listOfValues[599:612]
            self.nmCurso                             = listOfValues[612:722]
            self.nmGrau                              = listOfValues[722:744]
            self.cdConceitoCurso                     = listOfValues[744:745]
            self.anInicioPrevisto                    = listOfValues[745:749]
            
            self.dsSituacaoCurso                     = listOfValues[749:765]
            self.dtSituacaoCurso                     = listOfValues[765:770]
            self.idAddFotoProgramaIes                = listOfValues[770:776]
            self.idAddFotoPrograma                   = listOfValues[776:782]

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