import os
import common as aux

###################################################################################
######################## DB INITIALIZATION FUNCTIONS ##############################
###################################################################################

dbPath = "data.db"
dbHeaderPath = "data.h"
blankLines = list()

#Le o CSV e cria o arquivo do BD de Heap
def createHeapBD(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.padRecords(aux.readFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(dbPath):
        os.remove(dbPath)
    
    #make HEAD File
    aux.makeHEAD(dbHeaderPath, "Heap", 0)
    #preenche os valores direto no arquivo
    #file = open(aux.HeapPath, "w+")
    #file.write(aux.MakeHEADString("HEAP"))
    #file.close()
    
    recordCounter = 0
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row, checkPrimaryKey=False, showSuccessMsg=False)
        recordCounter +=1
    
    aux.updateHEAD(dbHeaderPath, "HEAP", recordCounter)
    






###################################################################################
##################################### HEAP ########################################
###################################################################################

###################################################################################
############################ HEAP - SELECT FUNCTIONS ##############################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#singleRecordSelection = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def HeapSelectRecord(colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = "", betweenTwoValues=False):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    recordFound = False
    endOfFile = False
    
    if betweenTwoValues:
        value = [str(x) for x in range(int(value[0]), int(value[1])+1, 1)]
        valueIsArray = True
    
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
            secondPrint = "AND " + secondColName + "=" + secondValue + " " if secondValuePresent else ""
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ") " + secondPrint + "LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            secondPrint = " AND " + secondColName + "=" + secondValue if secondValuePresent else ""
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ")" + secondPrint + ";\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRecord= 0#busca linear, sempre começamos do primeiro
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
                if secondColName!="" and not currentBlock[i][secondColumnIndex]==secondValue: continue
                print("Result found in record " + str(currentRecord+i) + "!")
                results += [currentBlock[i]]
                if singleRecordSelection:
                    recordFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRecord +=aux.blockSize
    
    print("End of search.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))
        
    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        return False
        
    else:
        print("Results found: \n")
        for result in results:
            print(result)
            print("\n")
        return True
    







#DONE
###################################################################################
############################ HEAP - INSERT FUNCTIONS ##############################
###################################################################################



#insere um valor novo na Heap(ou seja, no final dela)
def HeapInsertSingleRecord(listOfValues, checkPrimaryKey=True, showSuccessMsg=True):
    global blankLines

    if len(listOfValues) != len(aux.maxColSizesList):
        print("Erro: lista de valores recebidos não tem a mesma quantidade de campos da relação")
        return
    if checkPrimaryKey and not HeapSelectRecord('COD', aux.fillCod(listOfValues[0]), singleRecordSelection=True)==False:
        print(f"Não foi possível adicionar, pois já existe o código {listOfValues[0]}.")
        return
    
    if blankLines:
        row = aux.fillCod(listOfValues[0])
        for i in range(1, len(listOfValues)):
            row += aux.padString(listOfValues[i], aux.maxColSizesList[i])
        emptyRow = blankLines.pop()
        aux.markLineDeleted(emptyRow, dbPath, row=row)

    else:
        with open(dbPath, 'a') as file:
            #insere o CPF com seu proprio padding
            file.write(aux.fillCod(listOfValues[0]))
            #assumindo que estão na ordem correta já
            for i in range(1, len(listOfValues)):
                file.write(aux.padString(listOfValues[i], aux.maxColSizesList[i]))
            #por fim pulamos uma linha para o próximo registro
            file.write("\n")
    
    aux.updateHEAD(dbHeaderPath, "Heap", aux.queryHEADrecords(dbHeaderPath, aux.heapHeadSize)+1)
    if showSuccessMsg:
        print("Registro inserido com sucesso.")

def HeapInsertMultipleRecord(listOfRecords, checkPrimaryKey=True):
    for record in listOfRecords:
        HeapInsertSingleRecord(record, checkPrimaryKey)
    print("Todos os registros foram inseridos.")


def HeapMassInsertCSV(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.PadRecords(aux.ReadFromFile(csvFilePath))
    
    recordCounter = aux.queryHEADrecords(aux.HeapHeadPath, aux.heapHeadSize)
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        recordCounter +=1
    
    aux.updateHEAD(aux.HeapHeadPath, "HEAP", recordCounter)


###################################################################################
############################ HEAP - DELETE FUNCTIONS ##############################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#singleRecordDeletion = Retorna o PRIMEIRO registro onde 'colName' = à value se True
# def HeapDeleteRecord(colName, value, singleRecordDeletion = False, valueIsArray = False, secondColName = "", secondValue = ""):
#     numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
#     recordFound = False
#     endOfFile = False
    
#     indexesToDelete = []
    
#     values = ""
#     if valueIsArray:
#         for val in value:
#             values+= val + ", "
#         values = values[:len(values)-2]#tira ultima ', '
    
#     if colName not in aux.colHeadersList:
#         print("Error: Column name not found in relation.")
#         return
#     columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

#     secondValuePresent = False


#     secondColumnIndex = -1
#     if secondColName != "" and secondValue != "":
#         if secondColName not in aux.colHeadersList:
#             print("Error: Second column name not found in relation")
#             return
#         secondColumnIndex = aux.colHeadersList.index(secondColName)
#         secondValuePresent = True

#     print("\nRunning query: ")
#     if singleRecordDeletion:
#         if valueIsArray:
#             print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
#         else:
#             if secondValuePresent:
#                 print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
#             else:
#                 print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
#     else:
#         if valueIsArray:
#             print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
#         else:
#             if secondValuePresent:
#                 print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
#             else:
#                 print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

#     currentRecord= 0#busca linear, sempre começamos do primeiro
#     results = [] #retornar os deletados
#     while not (recordFound or endOfFile):
#         currentBlock = aux.fetchBlock(dbPath, currentRecord)#pega 5 registros a partir do registro atual
#         if currentBlock == []:
#             endOfFile = True
#             break
        
#         #mais um bloco varrido
#         numberOfBlocksUsed +=1
                      
#         for i in range(len(currentBlock)):
#             if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
#                 print("Result found in record " + str(currentRecord+i) + "!")
#                 results += [currentBlock[i]]
#                 #salvar index para deletar posteriormente
#                 indexesToDelete+=[currentRecord+i]

#                 if singleRecordDeletion:
#                     aux.deleteLineFromFile(currentRecord+i, dbPath)
#                     recordFound = True
#                     break
#         #se não é EOF e não encontrou registro, repete operação com outro bloco
#         currentRecord +=aux.blockSize
        
#     if results == []:
#         if valueIsArray:
#             print("Não foi encontrado registro com "+colName+ " in (" + values +")")
#         else:
#             print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
#     else:
#         print(indexesToDelete)
        
#         if not singleRecordDeletion:
#             for reg in reversed(indexesToDelete):
#                 aux.deleteLineFromFile(reg, dbPath)
#         print("\n\nRecords deleted: \n")
#         for result in results:
#             print(result)
#             print("\n")
    
#     print("End of query.")
#     print("Number of blocks fetched: " + str(numberOfBlocksUsed))

#     #updateHEAD with new number of records if there were deletions
#     if results != []:
#         aux.updateHEAD(dbHeaderPath, "Heap", aux.queryHEADrecords(dbHeaderPath, aux.heapHeadSize)-len(results))

def checkBlankLines(blankLines):
    percBlankLines = len(blankLines)*1.0/(aux.queryHEADrecords(dbHeaderPath, aux.heapHeadSize) + len(blankLines))
    print(blankLines, percBlankLines, len(blankLines))
    if percBlankLines>0.2 or len(blankLines)>2:
        for reg in sorted(blankLines)[::-1]:
            aux.deleteLineFromFile(reg, dbPath)
        return list()
    return blankLines


def HeapDeleteRecord(colName, value, singleRecordDeletion = False, valueIsArray = False, secondColName = "", secondValue = ""):
    global blankLines
    blankLines = checkBlankLines(blankLines)
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
                blankLines+=[currentRecord+i]

                if singleRecordDeletion:
                    aux.markLineDeleted(currentRecord+i, dbPath)
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
        
        if not singleRecordDeletion:
            for reg in reversed(indexesToDelete):
                aux.markLineDeleted(reg, dbPath)
        print("\n\nRecords deleted: \n")
        for result in results:
            print(result)
            print("\n")
    
    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    #updateHEAD with new number of records if there were deletions
    if results != []:
        aux.updateHEAD(dbHeaderPath, "Heap", aux.queryHEADrecords(dbHeaderPath, aux.heapHeadSize)-len(results))