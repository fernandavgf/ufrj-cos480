import csv
import os
import time
import datetime


paddingCharacter = "#"
blockSize = 5
bucketSize = 10            #Tamanho do bucket do hash (em blocos)
numberOfBuckets = 220      #Quantidade máxima de buckets
recordSize = 980+1         #153 chars + escape key


dicColHeaderType = {
        "AN_BASE": "INTEGER(4)",
        "NM_GRANDE_AREA_CONHECIMENTO": "VARCHAR(27)",
        "NM_AREA_CONHECIMENTO": "VARCHAR(42)",
        'NM_SUBAREA_CONHECIMENTO': "VARCHAR(59)", 
        'NM_ESPECIALIDADE': "VARCHAR(57)", 
        'CD_AREA_AVALIACAO': "INTEGER(2)",

        'NM_AREA_AVALIACAO': "VARCHAR(65)", 
        'CD_ENTIDADE_CAPES': "INTEGER(8)", 
        'CD_ENTIDADE_EMEC': "INTEGER(5)", 
        'SG_ENTIDADE_ENSINO': "VARCHAR(20)", 
        'NM_ENTIDADE_ENSINO': "VARCHAR(86)", 
        'CS_STATUS_JURIDICO': "VARCHAR(10)",

        'DS_DEPENDENCIA_ADMINISTRATIVA': "VARCHAR(7)",
        'DS_ORGANIZACAO_ACADEMICA': "VARCHAR(51)", 
        'NM_REGIAO': "VARCHAR(12)", 
        'SG_UF_PROGRAMA': "VARCHAR(2)", 
        'NM_MUNICIPIO_PROGRAMA_IES': "VARCHAR(25)",
        'CD_PROGRAMA_IES': "VARCHAR(13)",

        'NM_PROGRAMA_IES': "VARCHAR(104)", 
        'CD_CURSO_PPG': "VARCHAR(13)", 
        'NM_CURSO': "VARCHAR(110)", 
        'NM_GRAU': "VARCHAR(22)",
        'CD_CONCEITO_CURSO': "INTEGER(1)",
        'AN_INICIO_PREVISTO': "INTEGER(4)", 

        'DS_SITUACAO_CURSO': "VARCHAR(16)", 
        'DT_SITUACAO_CURSO': "VARCHAR(5)", 
        'ID_ADD_FOTO_PROGRAMA_IES': "INTEGER(6)",
        'ID_ADD_FOTO_PROGRAMA': "INTEGER(6)",
}

#Baseado no dic acima(CPF JOGADO PARA A PRIMEIRA POSICAO)
maxColSizesList = [4,27,42,59,57,2,65,8,5,20,86,10,7,51,12,2,25,13,104,13,110,221,4,16,5,6,6]

#Baseado no dic acima(e na ordem da lista acima, com CPF no início)
colHeadersList = ["AN_BASE", "NM_GRANDE_AREA_CONHECIMENTO", "NM_AREA_CONHECIMENTO", 'NM_SUBAREA_CONHECIMENTO', 'NM_ESPECIALIDADE', 'CD_AREA_AVALIACAO',
                  "NM_AREA_AVALIACAO", "CD_ENTIDADE_CAPES", "CD_ENTIDADE_EMEC", 'SG_ENTIDADE_ENSINO', 'NM_ENTIDADE_ENSINO', 'CS_STATUS_JURIDICO',
                  "DS_DEPENDENCIA_ADMINISTRATIVA", "DS_ORGANIZACAO_ACADEMICA", "NM_REGIAO", 'SG_UF_PROGRAMA', 'NM_MUNICIPIO_PROGRAMA_IES', 'CD_PROGRAMA_IES',
                  "NM_PROGRAMA_IES", "CD_CURSO_PPG", "NM_CURSO", 'NM_GRAU', 'CD_CONCEITO_CURSO', 'AN_INICIO_PREVISTO',
                  "DS_SITUACAO_CURSO", "DT_SITUACAO_CURSO", "ID_ADD_FOTO_PROGRAMA_IES", 'ID_ADD_FOTO_PROGRAMA']


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
        for j in range(len(listOfRecords[i])-1):
            listOfRecords[i][j] = padString(listOfRecords[i][j], maxColSizesList[j])
    return listOfRecords
