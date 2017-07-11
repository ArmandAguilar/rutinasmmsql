#!user/bin/python
from tokens import *
import unicodedata
import pymssql
import pypyodbc as pyodbc
import simplejson as json

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

def tDataJason(ListDataJson,periodo,listMaestrosA):
    print ('####################' +  str(periodo))
    temp = len(ListDataJson)
    ListDataJson = ListDataJson[:temp - 2]
    ListDataJson += ']}'
    data = json.loads(ListDataJson)
    for value in ListDataJson:
        #pass
        print str(value[0])
    #Here slide the array
    #for value in listMaestrosA:
        #pass
        #print  'Maestro : ' + str(valueNumMatestro) + ' Periodo :' + str(valueYears) + ' Trhoughput Maestro : ' + str(TrhoughputR)

    #print ListDataJson
    #print listMaestrosA

print('######################################### Begin Calculando Throughput ########################################')
# 2.- Run the Masters
#for valueNumMatestro in listMaestros:
    #2.2 .- read master in the list
#        for valueYears in listYears:
#            sqlT = 'SELECT ISNULL(Sum([Dias de produccion]),0) As DiasDeProduccion,ISNULL(Sum([Trabajo por programar]),0) As TrabajoPorProgramar , ISNULL(Sum([Margen Actual]),0) As MargenActual FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] Where [NumMaestro]=\'' + str(valueNumMatestro) + '\' and [PeriodoComparativo] = \'' + str(valueYears)  + '\''
#            con = pyodbc.connect(constr)
#            cur = con.cursor()
#            cur.execute(sqlT)
#            for valueData in cur:
#                DiasDeProduccion = valueData[0]
#                TrabajoPorProgramar = valueData[1]
#                MargenActual = valueData[2]
#                if MargenActual > 0:
#                    x = DiasDeProduccion + TrabajoPorProgramar
#                    TrhoughputR = MargenActual/x
#                else:
#                    TrhoughputR = TrabajoPorProgramar;
#                print  'Maestro : ' + str(valueNumMatestro) + ' Periodo :' + str(valueYears) + ' Trhoughput Maestro : ' + str(TrhoughputR)
#            con.commit()
#            con.close()

#2 .- We read the list and create the sql for calulate te thoriughput
# 2 .- Read the list for years


for valueYear in listYears:
    sql = 'SELECT [NumProyecto],[NumMaestro],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual,[PeriodoComparativo] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] where [PeriodoComparativo] =\'' + str(valueYear) + '\' order by NumMaestro'
    con = pyodbc.connect(constr)
    cur = con.cursor()
    cur.execute(sql)
    listMaestrosActivos = []
    ListDataJson = ''
    ListDataJson = '{"fields":['
    DNI = 0
    for value in cur:
        #passMSSQL
        listMaestrosActivos.insert(i,value[1])
        ListDataJson += '{"Id":"' + str(DNI) + '","NumProyecto":"' + str(value[0]) + '","NumMaestro":"' + str(value[1]) + '","DiasDeProduccion":"' + str(value[2]) + '","TrabajoPorProgramar":"' + str(value[3]) + '","MargenActual" : "' + str(value[4]) + '","PeriodoComparativo":"' + str(value[5]) + '"},' + '\n'
        DNI += 1
    #here procesing lotes
    listMaestrosA = list(set(listMaestrosActivos))
    tDataJason(ListDataJson,valueYear,listMaestrosA)


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
print('##################################### End Calculando Throughput ####################################')
