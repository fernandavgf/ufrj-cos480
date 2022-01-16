from pickle import FALSE
import hashFile as hashDB
import heapFile as heapDB

csvFilePath = "input.csv"
hashDB.createHashBD(csvFilePath)
hashDB.hashSelectRecord(["1230"])
hashDB.hashDeleteRecord(["1230"])

heapDB.createHeapBD(csvFilePath)
heapDB.HeapSelectRecord('COD', '1005', singleRecordSelection=True)
