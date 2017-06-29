#!user/bin/python
from tokens import *
import unicodedata
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

sql = 'SELECT [NumProyecto],[NomProyecto] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by NumMaestro desc'
conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
cur = conn.cursor()
cur.execute(sql)
for value in cur:
    #
    NumProyecto = value[0]
    NomProyecto = value[1]
    #NumMaestro = value[2]
    #Diasdeproduccion = value[3]
    #Trabajoporprogramar = value[4]
    #MargenActual = value[5]
    #PeriodoComparativo = value[6]
    mAction = fieldExist(NumProyecto,NomProyecto)
    if mAction == 'Si':
        #update
        Sql = 'update'
        print (Sql)
    else:
        Sql = 'insert'
        print(Sql)
conn.commit()
conn.close()

print('######################################## End Calculando Throughput #######################################')
