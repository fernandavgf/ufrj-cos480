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

def createHeapBD(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.padRecords(aux.readFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(dbPath):
        os.remove(dbPath)
    
    #make HEAD File
    aux.makeHEAD(dbHeaderPath, "Heap", 0)
    
    recordCounter = 0
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        heapInsertRecord(row)
        recordCounter +=1
    
    aux.makeHEAD(dbHeaderPath, "Heap", recordCounter)

def heapInsertRecord(record):
    with open(dbPath, 'a') as file:
        for i in range(0, len(record)):
            file.write(aux.padString(record[i], aux.maxColSizesList[i]))
        file.write("\n")
    aux.updateHEAD(dbHeaderPath, "Heap", aux.queryHEADrecords(dbHeaderPath, aux.heapHeadSize)+1)

def heapSelectRecord(colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = ""):
    numberOfBlocksUsed = 0
    recordFound = False
    endOfFile = False
    
    values = ""
    if valueIsArray:
        for val in value:
            values+= val + ", "
        values = values[:len(values)-2]#tira ultima ', '
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna
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
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRecord = 0#busca linear, sempre começamos do primeiro
    results = []
    while not (recordFound or endOfFile):
        currentBlock = aux.fetchBlock(dbPath, currentRecord)#pega 5 registros a partir do registro atual
        if currentBlock == []:
            endOfFile = True
            break
        
        #mais um bloco varrido
        numberOfBlocksUsed +=1
                      
        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in record " + str(currentRecord+i) + "!")
                print(currentBlock[i])
                results += [currentBlock[i]]
                if singleRecordSelection:
                    recordFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRecord +=aux.blockSize
        
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
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

def heapDeleteRecord(colName, value, singleRecordDeletion = False, valueIsArray = False, secondColName = "", secondValue = ""):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    recordFound = False
    endOfFile = False
    
    indexesToDelete = []
    
    values = ""
    if valueIsArray:
        for val in value:
            values+= val + ", "
        values = values[:len(values)-2]#tira ultima ', '
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    secondValuePresent = False


    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in aux.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = aux.colHeadersList.index(secondColName)
        secondValuePresent = True

    print("\nRunning query: ")
    if singleRecordDeletion:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRecord= 0#busca linear, sempre começamos do primeiro
    results = [] #retornar os deletados
    while not (recordFound or endOfFile):
        currentBlock = aux.fetchBlock(dbPath, currentRecord)#pega 5 registros a partir do registro atual
        if currentBlock == []:
            endOfFile = True
            break
        
        #mais um bloco varrido
        numberOfBlocksUsed +=1
                      
        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in record " + str(currentRecord+i) + "!")
                results += [currentBlock[i]]
                #salvar index para deletar posteriormente
                indexesToDelete+=[currentRecord+i]

                if singleRecordDeletion:
                    aux.deleteLineFromFile(currentRecord+i, dbPath)
                    recordFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRecord +=aux.blockSize
        
    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print(indexesToDelete)
        
        for reg in reversed(indexesToDelete):
            aux.deleteLineFromFile(reg, dbPath)
        print("\n\Records deleted: \n")
        for result in results:
            print(result)
            print("\n")
    
    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    #updateHEAD with new number of registries if there were deletions
    if results != []:
        aux.updateHEAD(dbPath, "Heap", aux.queryHEADrecords(dbPath, aux.heapHeadSize)-len(results))
    