import csv
import os
import time
import datetime


paddingCharacter = "#"
blockSize = 5
bucketSize = 10            #Tamanho do bucket do hash (em blocos)
numberOfBuckets = 220      #Quantidade máxima de buckets
recordSize = 153+1         #153 chars + escape key


dicColHeaderType = {
        "CPF": "INTEGER(11)",
        "SG_UF": "VARCHAR(2)",
        "CD_CARGO": "INTEGER(2)",
        'NR_CANDIDATO': "INTEGER(5)", 
        'NM_CANDIDATO': "VARCHAR(70)", 
        'NM_EMAIL': "VARCHAR(43)",
        'NR_PARTIDO': "INTEGER(2)", 
        'DT_NASCIMENTO': "DATE", 
        'CD_GENERO': "INTEGER(1)", 
        'CD_GRAU_INSTRUCAO': "INTEGER(1)", 
        'CD_ESTADO_CIVIL': "INTEGER(1)", 
        'CD_COR_RACA': "INTEGER(2)",
        'CD_OCUPACAO': "VARCHAR(3)"
}

#Baseado no dic acima(CPF JOGADO PARA A PRIMEIRA POSICAO)
maxColSizesList = [11,2,2,5,70,43,2,10,1,1,1,2,3]

#Baseado no dic acima(e na ordem da lista acima, com CPF no início)
colHeadersList = ["CPF", "SG_UF", "CD_CARGO", 'NR_CANDIDATO', 'NM_CANDIDATO', 'NM_EMAIL', 'NR_PARTIDO', 'DT_NASCIMENTO', 'CD_GENERO', 'CD_GRAU_INSTRUCAO', 'CD_ESTADO_CIVIL', 'CD_COR_RACA', 'CD_OCUPACAO']

#Baseado nos indices acima
relevantColsList = [10, 13, 16, 17, 20, 21, 27, 38, 41, 43, 45, 47, 49]

def isRelevantRow(rowNumber):
    #
    return rowNumber in relevantColsList

def fillCPF(cpf):
    return cpf.zfill(maxColSizesList[0])#tamanho de CPF e fixo


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
                    if isRelevantRow(i):
                        #Se for a coluna do CPF, coloca o mesmo no inicio da lista
                        if i == relevantColsList[4]:
                            finalRow.insert(0, fillCPF(row[i]))
                        else:
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
