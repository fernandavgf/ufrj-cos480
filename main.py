import fixedHeapFile as fixedHeapDB
import variableHeapFile as variableHeapDB
import hashFile as hashDB

csvFilePath = "input.csv"
#csvFilePath = "E:/Desktop/!CBD/Trabalho/ufrj-cos480/input.csv"

### FIXED HEAP
fixedHeapDB.createHeapBD(csvFilePath)

## Select
# Um registro com chave primária igual a X
fixedHeapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# Todos os registros com campo não sequencial igual a X
fixedHeapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# Todos os registros com campo chave entre dois valores
fixedHeapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# Todos os registros com campo não chave igual a X
fixedHeapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

## Insert
# Um registro
fixedHeapDB.HeapInsertSingleRecord(['3000', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO']) 

# Vários registros
fixedHeapDB.HeapInsertMultipleRecord(
    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO']],
    checkPrimaryKey=False) 

## Delete
# Um registro
fixedHeapDB.HeapDeleteRecord('COD', '1005', singleRecordDeletion=True)

# Vários registros
fixedHeapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')

#######################################################################################################################################

### VARIABLE HEAP
variableHeapDB.createHeapBD(csvFilePath)

## Select
# Um registro com chave primária igual a X
variableHeapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)

# Todos os registros com campo não sequencial igual a X
variableHeapDB.HeapSelectRecord('COD', ['1004', '1005'], secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ", valueIsArray=True)

# Todos os registros com campo chave entre dois valores
variableHeapDB.HeapSelectRecord('COD', ['1002', '1005'], betweenTwoValues=True)

# Todos os registros com campo não chave igual a X
variableHeapDB.HeapSelectRecord('SG_ENTIDADE_ENSINO', 'UFMG')

## Insert
# Um registro
variableHeapDB.HeapInsertSingleRecord(['9600', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO'], addPaddingCharacter=True) 

# Vários registros
variableHeapDB.HeapInsertMultipleRecord(
    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO E INFORMACAO']],
    checkPrimaryKey=False) 

## Delete
# Um registro
variableHeapDB.HeapDeleteRecord('COD', '1007', singleRecordDeletion=True)

# Vários registros
variableHeapDB.HeapDeleteRecord('SG_ENTIDADE_ENSINO', 'UFRJ')

#######################################################################################################################################

### HASH
hashDB.createHashBD(csvFilePath)

## Select
# Um registro com chave primária igual a X
hashDB.hashSelectRecord(["1230"])

# Todos os registros com campo não sequencial igual a X
hashDB.hashSelectRecord("COD", "1004", secondColName="SG_ENTIDADE_ENSINO", secondValue="UERJ")

# Todos os registros com campo chave entre dois valores
hashDB.hashSelectRecord(["1002", "1005"], betweenTwoValues=True)

# Todos os registros com campo não chave igual a X
hashDB.hashSelectRecord("SG_ENTIDADE_ENSINO", "UFRJ")

## Insert
# Um registro
hashDB.hashInsertRecord([["9000", "UFRJ", "UNIVERSIDADE FEDERAL DO RIO DE JANEIRO", "91038102022", "ENGENHARIA ELETRONICA"]]) 
hashDB.hashSelectRecord(["9100"])

# Vários registros
hashDB.hashInsertRecord(
    [['9100', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO'],
    ['9200', 'UFRJ', 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', '91038102022', 'ENGENHARIA DE COMPUTACAO']]) 
hashDB.hashSelectRecord(["9100","9200"])

## Delete
# Um registro
hashDB.hashDeleteRecord(["1005"])
hashDB.hashSelectRecord(["1005"])

# Vários registros
hashDB.hashDeleteRecord("SG_ENTIDADE_ENSINO","UFRJ")
hashDB.hashSelectRecord("SG_ENTIDADE_ENSINO", "UFRJ")