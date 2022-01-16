# -*- coding: utf-8 -*-

###Imports básicos
import os
import io
import math
import fileinput as fin
from dateutil import parser

import common as aux
dbpath = './data.db'
dbheaderpath = './data.h'


###Criação do BD ordenado
basecolumn = 1   #coluna base para a ordenação. 0 = COD, 1=sigla da instituição,...

def createOrderedDB(path):
    #Padding
    recordslist = aux.padRecords(aux.readFromFile(path))

    #Executando o Tomsort - Não é mergeSort mas usa um método já otimizado do Python, com a chave especificada na coluna
    recordslist = sorted(recordslist, key=sortKey) 

    #Deleta o arquivo se já existir
    if os.path.exists(dbpath):
        os.remove(dbpath)

    #Cria head do bd
    aux.makeHEAD(dbheaderpath, "Ordered", len(recordslist))

    #Preenchendo o arquivo
    file = open(dbpath, "w+")
    recordcounter = 0
    for j in recordslist:
        for i in range(len(j)):
            file.write(aux.padString(j[i], aux.maxColSizesList[i]))
        file.write("\n")
        recordcounter+=1
    file.close()
    nblocks = math.ceil(sum(1 for line in open('data.db'))/aux.blockSize)
    print(recordcounter,"registros com um total de",os.path.getsize(dbpath)+os.path.getsize(dbheaderpath), "bytes de memória secundária e",nblocks," blocos no total")

#####Funções relativas à atividade

#Arquivo de extensão para a inserção
extPath = "./extension.txt"

###Insert
def insertLineIntoFile(record):
    extension = open(extPath,"a+")

    #Escreve o registro no arquivo de extensão
    extension.write(record)

    #Todo seek só recoloca o ponteiro no início do arquivo
    extension.seek(0)
    nlines = sum(1 for line in extension)
    if nlines == aux.blockSize*2:  ###A cada dois blocos dentro do extension.txt o reorganize() é chamado
        extension.seek(0)

        #Transferindo do arquivo de extensão para o banco
        db = open(dbpath,"a+")
        for i in extension.readlines():
            db.write(i)
        db.close()

        #Limpando o arquivo de extensão
        extension.truncate(0)

        #Chamando a função de reorganizar
        print("Reorganize chamado")
        reorganize()

    extension.close()
    nblocks = math.ceil(sum(1 for line in open('data.db'))/aux.blockSize)
    extblocks = math.ceil(sum(1 for line in open('extension.txt'))/aux.blockSize)
    print("o banco usa", os.path.getsize(dbpath)+os.path.getsize(dbheaderpath),"bytes de memória com",nblocks,"blocos e o arquivo de extensão usa", os.path.getsize(extPath), "bytes de memória secundária com",extblocks,"blocos")

#Inserir multiplos possui a mesma implementação do simples, só não chama o simples por otimização
#Insere todos de uma vez e a checagem de tamanho do arquivo de extensão é feita com >= ao invés de ==
def insertMultiple(recordList):
    extension = open(extPath,"a+")
    for i in recordList:
        extension.write(i)
    extension.seek(0)
    nlines = sum(1 for line in extension)
    if nlines >= aux.blockSize*2:
        extension.seek(0)
        db = open(dbpath,"a+")
        for i in extension.readlines():
            db.write(i)
        db.close()
        extension.truncate(0)
        reorganize()
    extension.close()
    print("o banco usa", os.path.getsize(dbpath)+os.path.getsize(dbheaderpath),"bytes de memória e o arquivo de extensão usa", os.path.getsize(extPath), "bytes de memória secundária")


###Delete
# Função para deletar um único registro pelo ID
def DeleteSingleOrdered(ID):
    numberOfBlocksUsed = 0

    record,numberOfBlocksUsed = binarySearch(ID, math.ceil(sum(1 for line in open('data.db'))),fordeletion=True)
    if not record:
        print ("Registro não encontrado")
    else:
        print("Registro de número",record[:4],"marcado para deleção")
    print("Número de blocos varridos: ", math.ceil(numberOfBlocksUsed/aux.blockSize))

#Função para deletar vários registros de acordo com uma informação
def DeleteMultipleOrdered(data, ncolumn):
    numberOfBlocksUsed = 0 #n de acessos a blocos

    print("DELETE * FROM ORDERED WHERE", aux.colHeadersList[ncolumn], "=", data)
    toggleNone = True
    endchar = 0

    #Encontra o intervalo na string, roda até 5 vezes
    for i in range(ncolumn+1):
        startchar = endchar
        endchar += aux.maxColSizesList[i]

    #pega os valores do header para reescrever 
    #(de forma direta estava apresentando comportamento inesperado)
    headerr = open(dbheaderpath)
    headercontents = headerr.readlines()
    headerr.close()
    headerw = open(dbheaderpath,"w")
    for i in headercontents:
        headerw.write(i)
    #Varre o bd atrás de registros compatíveis
    for i in open(dbpath).readlines():
        if i[startchar:endchar] == data:
            toggleNone = 0
            headerw.write(str(i[:4])+" ")
        numberOfBlocksUsed += 1
    headerw.close()
    #Caso nenhum registro bata
    if toggleNone:
        print("Não há registros compatíveis\n")
    #Chama o reorganize toda vez que executa, diferente do single
    else:
        print("Chamando Reorganize")
        reorganize()

    print("Número de blocos varridos: ", math.ceil(numberOfBlocksUsed/aux.blockSize))

###Reorganizar
def reorganize():
    file = open(dbpath,"a+")
    file.seek(0)
    list = file.readlines()

    #Ordenando com Tomsort
    list = sorted(list, key=sortKeyReorganized) 
    file.truncate(0)

    #Detectando e limpando os marcados para deleção
    headerfile = open(dbheaderpath,"r+")
    headerR = headerfile.readlines()

    #pegando os valores únicos em ordem decrescente (para deletar os valores certos)
    todeletelist = sorted(set(headerR[-1].split(" ")),reverse=True)
    for i in list:
        #reescrevendo no banco, em ordem
        if i[:4] not in todeletelist:
            file.write(i)
    file.close()

    #limpando os valores alterados a partir do header
    headerfile.seek(0)
    headerfile.truncate(0)
    for i in headerR[:-1]:
        headerfile.write(i)
    headerfile.write("\n")
    headerfile.close()


###Select

##Select 1 (Find)

def OrderedSelectSingleRecord(value):
    numberOfBlocksUsed = 0 #n de acessos a blocos

    print("SELECT * FROM ORDERED WHERE COD =", value)

    #Total de registros do BD
    nrecords = math.ceil(sum(1 for line in open('data.db')))
    
    #Executa busca binária
    record,numberOfBlocksUsed = binarySearch(value, nrecords)

    if(record):
        print("Registro encontrado: ")
        print(record)
    else:
        print("Registro não encontrado")

    print("Número de blocos varridos: " + str(numberOfBlocksUsed))

##Select all (FindAll Unique)
def OrderedSelectMultipleRecords(valueList):
    numberOfBlocksUsed = 0 #n de acessos a blocos
    totalblocks = 0
    recordList = []
    print("SELECT * FROM ORDERED WHERE COD IN", valueList)

    #Total de registros do BD
    nrecords = math.ceil(sum(1 for line in open('data.db')))
    
    #Executa busca binária para cada elemento da lista e imprime caso encontrado
    for i in range(len(valueList)):
        record,numberOfBlocksUsed = binarySearch(valueList[i], nrecords)
        if record: 
            print("Registro", valueList[i],"encontrado:")
            print(record)
        else:
            print("Registro", valueList[i],"não encontrado")
        totalblocks += numberOfBlocksUsed

    print("Número de blocos varridos: " + str(totalblocks))

##Select all in interval (FindAll Between)
def OrderedSelectInterval(startvalue,endvalue):
    numberOfBlocksUsed = 0 #n de acessos a blocos

    print("SELECT * FROM ORDERED WHERE COD BETWEEN", startvalue, "and", endvalue)
    print("\nImprimindo os valores dentro do intervalo:")

    #Varre o bd atrás de dados que estão dentro do intervalo;
    #é necessário caso a coluna base para a ordenação não seja a coluna COD
    for i in open(dbpath).readlines():
        if int(i[:4]) in range(int(startvalue),int(endvalue)):
            print(i)
        numberOfBlocksUsed += 1

    print("Número de blocos varridos: ", math.ceil(numberOfBlocksUsed/aux.blockSize))

##Select all from not key value (FindAll NotUnique)
def OrderedSelectNotUnique(data, ncolumn):
    numberOfBlocksUsed = 0 #n de acessos a blocos

    print("SELECT * FROM ORDERED WHERE", aux.colHeadersList[ncolumn], "=", data)
    print("\nImprimindo os registros com essa característica:")
    toggleNone = True
    endchar = 0

    #Encontra o intervalo na string, roda até 5 vezes
    for i in range(ncolumn+1):
        startchar = endchar
        endchar += aux.maxColSizesList[i]

    #Varre o bd atrás de registros compatíveis
    for i in open(dbpath).readlines():
        if i[startchar:endchar] == data:
            toggleNone = 0
            print(i)
        numberOfBlocksUsed += 1

    #Caso nenhum registro bata
    if toggleNone:
        print("Não há registros compatíveis\n")

    print("Número de blocos varridos: ", math.ceil(numberOfBlocksUsed/aux.blockSize))

###Funções auxiliares
def sortKey(lista):
    return lista[basecolumn]

def sortKeyReorganized(lista):
    cont = 0
    for i in range(basecolumn):
        cont+= aux.maxColSizesList[i]
    return lista[cont:]

def binarySearch(value, nrecords, fordeletion=False):
    numberOfBlocksUsed = 0  
    
    #Intervalo de busca dos blocos
    left = 0
    right = nrecords

    #Ajustando os blocos para leitura ordenada (necessário caso basecolumn não for 0)
    dbfile = open(dbpath)
    regs = dbfile.readlines()
    if basecolumn != 0:
        regs = sorted(regs)
    recsize = len(regs)

    while left <= right: 
        numberOfBlocksUsed += 1

        # Encontra o bloco do meio
        middle = int((left+right)/2); 

        # Busca o registro
        if middle < recsize:
            midID = regs[middle][:4]
        else:
            # Retorna 0 se não achar
            dbfile.close()
            return 0, numberOfBlocksUsed           

        if midID == value:  
            record = regs[middle]
            dbfile.close()
            if fordeletion:
                #Escrevendo no header do banco os pontos de deleção futura que ocorrerão no reorganize
                mrheader=open(dbheaderpath,"a")
                mrheader.write(record[:4]+" ")
                mrheader.close()
                header=open(dbheaderpath)
                if len(header.readlines()[-1].split(" ")) > int(len(regs)/1000):
                    print("Reorganize chamado")
                    reorganize()
            return record, numberOfBlocksUsed
        else:
            #Selecionando o próximo lado 
            if(value > midID):
                left = middle + 1
            else:
                right = middle - 1
    dbfile.close()            
    
    # Retorna 0. Essa linha não deve ocorrer, mas está presente para evitar imprevistos
    return 0, numberOfBlocksUsed