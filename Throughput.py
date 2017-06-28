#!user/bin/python
from tokens import *
import unicodedata
import pypyodbc as pyodbc
import pymssql


# 1 .- Step get a list of proyect of  to insert
# 2 .- Search the if proyect exit in the table
#   2.1 if Proyecto exist update data
#   2.2 if proyect dont exist insert data

def fieldExist(NumProyect,NumMaster):
    Accion = 'No'
    sql_buscar = 'SELECT [Id] FROM [SAP].[dbo].[AAAThroughput] Where [NumProyecto] = \'' + str(NumProyect) + '\' and [NumMatestro] = \'' + str(NumMaster) + '\''
    conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
    cur = conn.cursor()
    cur.execute(sql_buscar)
    for value in cur:
         Accion = 'Si'
    conn.commit()
    conn.close()

    return Accion

def insertTroughtPut(sql):
    conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()

def upadteTroughtPut(sql):
    conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()

print('###################################### Begin Calculando Throughput ######################################')

sql = "SELECT [NumProyecto],[NomProyecto],[NumMaestro],[Dias de produccion],[Trabajo por programar],[Margen Actual] ,[PeriodoComparativo] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by NumMaestro desc";
conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
cur = conn.cursor()
cur.execute(sql)
for value in cur:

      mAction = fieldExist(value[0],value[2])
      if mAction == 'Si':
          #update
          Sql = 'update'
          print (Sql)
     else
        #Insetr
        Sql = 'Update'
        print ()
conn.commit()
conn.close()

print('####################################### End Calculando Throughput #######################################')
