#!user/bin/python
from tokens import *
import unicodedata
import pymssql
import pypyodbc as pyodbc
import json

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
# 1 .- We create a two list NumMaestro and Peridos

# 1.1 .- get NumMeatros
listMaestros = []
k = 0
sql = 'SELECT DISTINCT([NumMaestro]) FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
for value in cur:
    listMaestros.insert(k,value[0])
con.commit()
con.close()
print listMaestros

#1.2 .-  get years
listYears = []
i = 0
sql = 'SELECT DISTINCT(ISNULL([PeriodoComparativo],1999)) FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
for value in cur:
    listYears.insert(i,value[0])
con.commit()
con.close()
print listYears
ListDataJson = '{"data":['
DNI = 1
print('######################################### Begin Calculando Throughput ########################################')
#2 .- We read the list and create the sql for calulate te thoriughput
# 2 .- Read the list for years
for valueYear in listYears:
    sql = 'SELECT [NumProyecto],[NumMaestro],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual,[PeriodoComparativo] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] where [PeriodoComparativo] =\'' + str(valueYear) + '\' order by NumMaestro'
    print (sql)
    con = pyodbc.connect(constr)
    cur = con.cursor()
    cur.execute(sql)
    for value in cur:
        #passMSSQL
        ListDataJson += '{"Id" : "' + str(DNI) + '","NumProyecto" : "' + str(value[0]) + '","NumMaestro" : "' + str(value[1]) + '","DiasDeProduccion" : "' + str(value[2]) + '","TrabajoPorProgramar" : "' + str(value[3]) + '","MargenActual" : "' + str(value[4]) + '","PeriodoComparativo" : "' + str(value[5]) + '"},'
        DNI += 1
ListDataJson += ']}'

data = json.dumps(ListDataJson)
dataJson = json.loads(data)
#print(dataJson)
for value in dataJson[1]:
    print (str(value['Id']))
#sql = 'SELECT [NumProyecto],[NumMaestro],[Dias de produccion],[Trabajo por programar],[Margen Actual],[PeriodoComparativo] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]'
#con = pyodbc.connect(constr)
#cur = con.cursor()
#cur.execute(sql)
#for value in cur:
#    print ('Gos ....')
#    NumProyecto = value[0]
#    NumMaestro = value[1]
#    Diasdeproduccion = value[2]
#    Trabajoporprogramar = value[3]
#    MargenActual = value[4]
#    PeriodoComparativo = value[5]
#    if NumMaestro > 0:
#        mAction = fieldExist(NumProyecto,NumMaestro)
#        if mAction == 'Si':
#            #update
#            Sql = 'update'
#            print (Sql)
#        else:
#            Sql = 'INSERT INTO [SAP].[dbo].[AAAThroughput] ([NumProyecto],[NumMatestro],[ThroughputMaestro],[ThroughputCliente]) VALUES(\'' + str(NumProyecto) + '\',\'' + str(NumMaestro) + '\',<ThroughputMaestro, float,>,<ThroughputCliente, float,>)'
#            print(Sql)
#    else:
#        print('Proyecto Sin Maestro : ' +  str(NumProyecto))
#con.commit()
#con.close()
print('####################################### End Calculando Throughput #######################################')
