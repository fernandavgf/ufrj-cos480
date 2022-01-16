from pickle import FALSE
import hashFile as hashDB
import heapFile as heapDB

csvFilePath = "E:/Desktop/!CBD/Trabalho/ufrj-cos480/input.csv"
#hashDB.createHashBD(csvFilePath)
#hashDB.hashDeleteRecord(["1230"])
#hashDB.hashSelectRecord(["1230"])

heapDB.createHeapBD(csvFilePath)
heapDB.heapSelectRecord("COD","1230")
heapDB.heapDeleteRecord("COD","1230")
