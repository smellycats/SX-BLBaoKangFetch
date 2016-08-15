import cx_Oracle 
from mysqldb import HMysql

mysql = HMysql('localhost','root','','127.0.0.1')
mysql.login()

def OperationToString(operation): 
    operations = [] 
    if operation & cx_Oracle.OPCODE_INSERT: 
        operations.append("insert") 
    if operation & cx_Oracle.OPCODE_DELETE: 
        operations.append("delete") 
    if operation & cx_Oracle.OPCODE_UPDATE: 
        operations.append("update") 
    if operation & cx_Oracle.OPCODE_ALTER: 
        operations.append("alter") 
    if operation & cx_Oracle.OPCODE_DROP: 
        operations.append("drop") 
    if operation & cx_Oracle.OPCODE_ALLOPS: 
        operations.append("all operations") 
    return ", ".join(operations) 

def OnChanges(message): 
    print "Message received" 
    print "    Database Name:", message.dbname 
    print "    Tables:" 
    for table in message.tables: 
        print "        Name:", table.name, 
        print "        Operations:",
        print OperationToString(table.operation) 
        if table.rows is None \
                or table.operation & cx_Oracle.OPCODE_ALLROWS:
            pass
            #print "        Rows: all rows" 
        else: 
            #print "        Rows:"
            #print 'Operation: insert'
            values = []
            for row in table.rows:
                if OperationToString(row.operation) == 'insert':
                    print 'Operation: insert'
                    values.append((row.rowid))
            #mysql.addJGCL(values)
                    #print "            Rowid:", row.rowid 
                    #print "            Operation:", 
                    #print OperationToString(row.operation) 

#connection = cx_Oracle.Connection("snapall/bkxtjc@snapall",
        #events = True)

connection = cx_Oracle.connect('kakou','kakou','localhost'+':'+'1521'+'/'+'kakou',events = True)

sql = "select * from CLTX"
while 1:
    try:
        subscriptionAll = connection.subscribe(callback = OnChanges)
        subscriptionAll.registerquery(sql) 
        subscriptionInsertUpdate = \
                connection.subscribe(callback = OnChanges, 
                operations = cx_Oracle.OPCODE_INSERT ,\
                rowids = True)
        print 'test'
        subscriptionInsertUpdate.registerquery(sql)
        import time
        time.sleep(1)
    except Exception,e:
        print e
#raw_input("Hit enter to terminate...\n")


    
