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
    if value == '1999':
        valuesyears = 0
    else:
        listYears.insert(i,value[0])
con.commit()
con.close()

def MargenXMaestros(NoMaestros):
    #
    Margen = 0
    sql = 'SELECT SUM([Margen Actual]) As Margen FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] WHERE [NumMaestro] = \'' + str(NoMaestros) + '\''
    con = pyodbc.connect(constr)
    cur = con.cursor()
    cur.execute(sql)
    for value in cur:
        Marge = value[0]
    con.commit()
    con.close()

    return Margen


def tDataJason(ListDataJson,periodo,listMaestrosA,ListMargenJson):
    print ('####################' +  str(periodo))
    temp = len(ListDataJson)
    ListDataJson = ListDataJson[:temp - 2]
    ListDataJson += ']}'
    data = json.loads(ListDataJson)
    print str(ListMargenJson)
    #datamargen = json.loads(ListMargenJson)
    #Here slide the array
    for valueListMaestros in listMaestrosA:
        #pass
        DiasDeProduccion = 0
        TrabajoPorProgramar = 0
        MargenActual = 0
        TrhoughputR = 0
        MargenXMaestro = 0
        for valueJson in data['fields']:
            if str(valueListMaestros) == (valueJson['NumMaestro']):
                #pass
                DiasDeProduccion += valueJson['DiasDeProduccion']
                TrabajoPorProgramar += valueJson['TrabajoPorProgramar']
                MargenActual += valueJson['MargenActual']
        #MargenXMaestro = MargenXMaestros(str(valueListMaestros))
        for valuemargen in datamargen['fields']:

            if  valueListMaestros == valuemargen['NumMaestro']:
                MargenXMaestro += valuemargen['MargenActual']
            else:
                MargenXMaestro = 0

        if MargenXMaestro > 0:
            x = DiasDeProduccion + TrabajoPorProgramar
            TrhoughputR = MargenXMaestro/x
        else:
            TrhoughputR = 0
        print ('DiasDeProduccion :' +  str(DiasDeProduccion) + ' TrabajoPorProgramar : ' + str(TrabajoPorProgramar) + ' MargenActual :' + str(MargenActual) + ' NoMaestro :' + str(valueListMaestros) + ' Thoriughput Maestro : $' + str(TrhoughputR) + ' MargenXMaestro : $' + str(MargenXMaestro))

    #print ListDataJson
    #print listMaestrosA

print('######################################### Begin Calculando Throughput ########################################')

#here make a JSON for the Margen
#Print Calculando Margen
#for valuelistMaster in listMaestros:
#    Margen = 0
#    sql = 'SELECT SUM([Margen Actual]) As Margen FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] WHERE [NumMaestro] = \'' + str(valuelistMaster) + '\''
#    con = pyodbc.connect(constr)
#    cur = con.cursor()
#    cur.execute(sql)
#    for value in cur:
#        Margen = value[0]
#    con.commit()
#    con.close()
#    print ('NumMaestro : '  + str(valuelistMaster) + 'Margen:' + str(Margen))

print ('#Meking..... Json for Margen')
sql = 'SELECT [NumMaestro],[Margen Actual] As MargenActual FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]  order by NumMaestro desc'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
listMaestrosActivos = []
ListDataMargenJson = ''
ListDataMargenJson = '{"fields":['
DNIM = 0
for value in cur:

    if value[0] > 0:
        ListDataMargenJson += '{"Id":"' + str(DNIM) + '","NumMaestro" : "' + str(value[0]) + ',"MargenActual" : ' + str(value[1]) + '},' + '\n'
        DNIM += 1
        #print str(ListDataMargenJson)

temp = len(ListDataMargenJson)
ListDataMargenJson = ListDataMargenJson[:temp - 2]
ListDataMargenJson += ']}'


print('#Send Data for calculate Thoriughput')
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
        if value[1] > 0:
            listMaestrosActivos.insert(i,value[1])
            ListDataJson += '{"Id":"' + str(DNI) + '","NumProyecto":"' + str(value[0]) + '","NumMaestro":"' + str(value[1]) + '","DiasDeProduccion": ' + str(value[2]) + ',"TrabajoPorProgramar":' + str(value[3]) + ',"MargenActual" : ' + str(value[4]) + ',"PeriodoComparativo":' + str(value[5]) + '},' + '\n'
            DNI += 1
    #here procesing lotes
    listMaestrosA = list(set(listMaestrosActivos))
    tDataJason(ListDataJson,valueYear,listMaestrosA,ListDataMargenJson)

print('##################################### End Calculando Throughput ####################################')
