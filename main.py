from pickle import FALSE
from tkinter.tix import Select
import hashFile as hashDB
import fixedHeapFile as fixedHeapDB
import variableHeapFile as variableHeapDB

csvFilePath = "input.csv"
# hashDB.createHashBD(csvFilePath)
# hashDB.hashSelectRecord(["1230"])
# hashDB.hashDeleteRecord(["1230"])

# # Fixed Heap
# fixedHeapDB.createHeapBD(csvFilePath)

# ## Função Select
# # Um registro com chave primária igual a X
# fixedHeapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# # Todos os registros com campo não sequencial igual a X
# fixedHeapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# # Todos os registros com campo chave entre dois valores
# fixedHeapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# # TOdos os registros com campo não chave igual a X
# fixedHeapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

# ## Função Insert

# # Um registro
# fixedHeapDB.HeapInsertSingleRecord(['3000', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO']) 

# # Vários registros
# fixedHeapDB.HeapInsertMultipleRecord(
#     [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
#     ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO']],
#     checkPrimaryKey=False) 

# ## Função Delete

# # Um registro
# fixedHeapDB.HeapDeleteRecord('COD', '1005', singleRecordDeletion=True)

# # Vários registros
# fixedHeapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')

# Fixed Heap
variableHeapDB.createHeapBD(csvFilePath)

## Função Select
# Um registro com chave primária igual a X
variableHeapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# Todos os registros com campo não sequencial igual a X
variableHeapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# Todos os registros com campo chave entre dois valores
variableHeapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# TOdos os registros com campo não chave igual a X
variableHeapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

## Função Insert

# Um registro
variableHeapDB.HeapInsertSingleRecord(['9600', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO'], addPaddingCharacter=True) 

# Vários registros
variableHeapDB.HeapInsertMultipleRecord(
    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO']],
    checkPrimaryKey=False) 

## Função Delete

# Um registro
variableHeapDB.HeapDeleteRecord('COD', '1005', singleRecordDeletion=True)

# Vários registros
variableHeapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')