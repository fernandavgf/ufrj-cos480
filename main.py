from pickle import FALSE
from tkinter.tix import Select
import hashFile as hashDB
import heapFile as heapDB

csvFilePath = "input.csv"
hashDB.createHashBD(csvFilePath)
hashDB.hashSelectRecord(["1230"])
hashDB.hashDeleteRecord(["1230"])

heapDB.createHeapBD(csvFilePath)

## Função Select
# Um registro com chave primária igual a X
heapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# Todos os registros com campo não sequencial igual a X
heapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# Todos os registros com campo chave entre dois valores
heapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# TOdos os registros com campo não chave igual a X
heapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

## Função Insert

# Um registro
heapDB.HeapInsertSingleRecord(['3000', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO']) 

# Vários registros
heapDB.HeapInsertMultipleRecord(
    [['9000', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO'],
    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTAÇÃO E INFORMAÇÂO']],
    checkPrimaryKey=False) 

## Função Delete

# Um registro
heapDB.HeapDeleteRecord('COD', '1005', singleRecordDeletion=True)

# Vários registros
heapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')