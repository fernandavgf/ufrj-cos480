import hashFile as hashDB
import heapFile as heapDB
from pickle import FALSE
from tkinter.tix import Select

csvFilePath = "E:/Desktop/!CBD/Trabalho/ufrj-cos480/input.csv"
#csvFilePath = "input.csv"

# Hash
#hashDB.createHashBD(csvFilePath)

## Função Select
# Um registro com chave primária igual a X
#hashDB.hashSelectRecord(["1230"])

# Todos os registros com campo não sequencial igual a X
#hashDB.hashSelectRecord("COD", "1004", secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ")


# Todos os registros com campo chave entre dois valores
#hashDB.hashSelectRecord(["1002", "1005"], betweenTwoValues=True)

# Tpdos os registros com campo não chave igual a X
#hashDB.hashSelectRecord("SG_ENTIDADE_ENSINO", "UFRJ")

## Função Insert

# Um registro
#hashDB.hashInsertRecord([["9000", "UFRJ", "UNIVERSIDADE FEDERAL DO RIO DE JANEIRO", "91038102022", "ENGENHARIA ELETRONICA"]]) 
#hashDB.hashSelectRecord(["9100"])

# Vários registros
#hashDB.hashInsertRecord(
#    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
#    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO']]) 
#hashDB.hashSelectRecord(["9100","9200"])

## Função Delete

# Um registro
#hashDB.hashDeleteRecord(["1005"])
#hashDB.hashSelectRecord(["1005"])
# Vários registros
hashDB.hashDeleteRecord("SG_ENTIDADE_ENSINO","UFRJ")
hashDB.hashSelectRecord("SG_ENTIDADE_ENSINO", "UFRJ")
########################################################################################################################

# Heap
#heapDB.createHeapBD(csvFilePath)

## Função Select
# Um registro com chave primária igual a X
#heapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# Todos os registros com campo não sequencial igual a X
#heapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# Todos os registros com campo chave entre dois valores
#heapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# Todos os registros com campo não chave igual a X
#heapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

## Função Insert

# Um registro
#heapDB.HeapInsertSingleRecord(['3000', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO']) 

# Vários registros
#heapDB.HeapInsertMultipleRecord(
#    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO'],
#    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO E INFORMAÇÂO']],
#    checkPrimaryKey=False) 

## Função Delete

# Um registro
#heapDB.HeapDeleteRecord('COD', '1005', singleRecordDeletion=True)

# Vários registros
#heapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')