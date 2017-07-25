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

def SqlTroughtPut(sql):
    conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()

def tDataJason(ListDataJson,periodo,listMaestrosA):
    print ('####################' +  str(periodo))
    temp = len(ListDataJson)
    ListDataJson = ListDataJson[:temp - 2]
    ListDataJson += ']}'
    data = json.loads(ListDataJson)
    #Here slide the array
    insertJSON = ''
    insertJSON = '{"insertData":['
    for valueListMaestros in listMaestrosA:

        DiasDeProduccion = 0
        TrabajoPorProgramar = 0
        MargenActual = 0
        TrhoughputR = 0
        MargenXMaestro = 0
        for valueJson in data['fields']:
            if str(valueListMaestros) == (valueJson['NumMaestro']):
                DiasDeProduccion += valueJson['DiasDeProduccion']
                TrabajoPorProgramar += valueJson['TrabajoPorProgramar']
                MargenXMaestro += valueJson['MargenActual']

        x = DiasDeProduccion + TrabajoPorProgramar
        if x > 0:
            TrhoughputR = MargenXMaestro/x
        else:
            TrhoughputR = 0

        insertJSON += '{"NumMaestro":' + str(valueListMaestros) + ',"MargenMaestro":' + str(MargenXMaestro) + ',"TrhoughputMaestro":' + str(TrhoughputR)  + '},' + '\n'
    temp = len(insertJSON)
    insertJSON = insertJSON[:temp - 2]
    insertJSON += ']}'
    print insertJSON
    dataTrhoughputMaestro = json.loads(insertJSON)
    for valueTM in dataTrhoughputMaestro['insertData']:
        #pass
        Sql = 'INSERT INTO [SAP].[dbo].[AA_ThroughtputMatestro] VALUES (\'' + str(valueTM['NumMaestro']) + '\',\'' + str(valueTM['TrhoughputMaestro']) + '\',\'' + str(periodo) + '\',\'' + str(valueTM['MargenMaestro']) + '\')'
        print Sql
# 1 .- We create a two list NumMaestro and Peridos

# 1.1 .- get NumMeatros
listMaestros = []
k = 0
sql = 'SELECT DISTINCT([NumMaestro]) FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by [NumMaestro] desc'
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

print('######################################### Begin Calculando Throughput ########################################')
# 2 .- We create the data for the json files
# 2.1 .- Here we create the json for sum the (MargeActual) od mssql of  vista RV-ESTADOPROYECTOS-AA-Throughput
print('')
print ('#Meking Json for Margen Projects')
sql = 'SELECT [NumMaestro],[Margen Actual] As MargenActual ,[Empresa],[NumProyecto] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]  order by NumMaestro desc'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
listMaestrosActivos = []
ListDataMargenJson = ''
ListDataMargenJson = '{"fields":['
DNIM = 0
for value in cur:
    if value[0] > 0:
        ListDataMargenJson += '{"NoProyecto":"' + str(value[3]) + '","NumMaestro" : ' + str(value[0]) + ',"MargenActual" : ' + str(value[1]) + ',"Empresa" : "' + str(value[2]) + '"},' + '\n'
        DNIM += 1
con.commit()
con.close()
temp = len(ListDataMargenJson)
ListDataMargenJson = ListDataMargenJson[:temp - 2]
ListDataMargenJson += ']}'

#2.2 Here calculate the Though put for companny
#### Don`t touch this code
#Here we create the json of table RV-ESTADOPROYECTOS-AA-Throughput this json we use for read all the projects
print('')
print('####Send Data for calculate Throughtput')
for valueYear in listYears:
    sql = 'SELECT [NumProyecto],[NumMaestro],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual,[PeriodoComparativo],[Empresa] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] where [PeriodoComparativo] =\'' + str(valueYear) + '\' order by NumMaestro'
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
            ListDataJson += '{"Id":"' + str(DNI) + '","NumProyecto":"' + str(value[0]) + '","NumMaestro":"' + str(value[1]) + '","DiasDeProduccion":' + str(value[2]) + ',"TrabajoPorProgramar":' + str(value[3]) + ',"MargenActual":' + str(value[4]) + ',"PeriodoComparativo":' + str(value[5]) + ',"Empresa":"' + str(value[6]) + '"},' + '\n'
            DNI += 1
    con.commit()
    con.close()
    #here procesing all json in the function tDatJson()
    listMaestrosA = list(set(listMaestrosActivos))
    if valueYear > 1999 :
        tDataJason(ListDataJson,valueYear,listMaestrosA)

print('##################################### End Calculando Throughput ######################################')
