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

def ThroughputClients(Empresa,Periodo,NumMaestro,ListDataJsonCompanys):
    dataCompanys = json.loads(ListDataJsonCompanys)
    MargenXMaestroEmpresa = 0
    TrhoughputRC = 0
    DiasDeProduccion = 0
    TrabajoPorProgramar  = 0
    for value in dataCompanys['Companys']:
        if value['Empresa'] == str(Empresa):
            if value['PeriodoComparativo'] == Periodo:
                if int(value['NumMaestro']) == int(NumMaestro):
                    print('#####Armando#####NumProyecto' + str(value['NumProyecto']) + 'NumMaestro : ' + str(value['NumMaestro']) + 'Empresa :' + str(value['Empresa']) + 'Margen Actual:$' +  str(value['MargenActual']) + 'Periodo Comparativo :' + str(value['PeriodoComparativo']))
                    MargenXMaestroEmpresa += value['MargenActual']
                    DiasDeProduccion += value['DiasDeProduccion']
                    TrabajoPorProgramar += value['TrabajoPorProgramar']
                    if MargenXMaestroEmpresa > 0:
                        x = DiasDeProduccion + TrabajoPorProgramar
                        TrhoughputRC = MargenXMaestroEmpresa/x
                    else:
                        TrhoughputRC = 0
    return TrhoughputRC

def tDataJason(ListDataJson,periodo,listMaestrosA,ListMargenJson,ListDataJsonCompanys):
    print ('####################' +  str(periodo))
    temp = len(ListDataJson)
    ListDataJson = ListDataJson[:temp - 2]
    ListDataJson += ']}'
    data = json.loads(ListDataJson)
    datamargen = json.loads(ListMargenJson)
    #Here slide the array
    insertJSON = '{"insert":['
    for valueListMaestros in listMaestrosA:
        #pass
        DiasDeProduccion = 0
        TrabajoPorProgramar = 0
        MargenActual = 0
        TrhoughputR = 0
        MargenXMaestro = 0
        ThroughputC = 0
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
        #print('## Margen Actual : $' + str(MargenXMaestro) + '/ (DiasDeProduccion : ' + str(DiasDeProduccion) + ' + TrabajoPorProgramar :' + str(TrabajoPorProgramar) + ')')
        #ThroughputRClients = ThroughputClients(valueJson['Empresa'],periodo,valueListMaestros,ListDataJsonCompanys)
        #print ('NoMaestro :' + str(valueListMaestros)  + ' Throughput Maestro : $' + str(TrhoughputR))
        #print('############Empresa:' + str(valueJson['Empresa']) + ' NumMaestro: ' + valueJson['NumMaestro'] + ' Periodo:' + str(periodo))
    #print ListDataJson
    #print listMaestrosA
    temp = len(insertJSON)
    insertJSON = insertJSON[:temp - 2]
    insertJSON += ']}'
    print insertJSON


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

#2.3 Here calculate the Though put for companny
print('')
print ('#Meking Json for Margen Clients')
#2.3.1.- We create a  list of only compannys
listCompanys = []
i = 0
sql = 'SELECT [Empresa] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by [Empresa] asc'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
for value in cur:
    listCompanys.insert(i,value[0])
con.commit()
con.close()
#Here delete duplicate elements
listCompanysA = list(set(listCompanys))


#Here we create us a new Json with all data of  (RV-ESTADOPROYECTOS-AA-Throughput)  with this data we go a new json for slice and comparte companys
sql = 'SELECT [NumMaestro],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual, ISNULL([PeriodoComparativo],1999) As PeriodoComparativo,[Empresa],[NumProyecto]  FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by NumMaestro'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
ListDataJsonCompanys = ''
ListDataJsonCompanys = '{"Companys":['
DNI = 0
for value in cur:
    if value[0] > 0:
        ListDataJsonCompanys += '{"NumProyecto":' +  str(value[6])  + ',"NumMaestro":' + str(value[0]) + ',"DiasDeProduccion": ' + str(value[1]) + ',"TrabajoPorProgramar":' + str(value[2]) + ',"MargenActual": ' + str(value[3]) + ',"PeriodoComparativo":' + str(value[4]) + ',"Empresa":"' + str(value[5]) + '"},' + '\n'

con.commit()
con.close()
temp = len(ListDataJsonCompanys)
ListDataJsonCompanys = ListDataJsonCompanys[:temp - 2]
ListDataJsonCompanys += ']}'


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
            if value[5] == 2017:
                listMaestrosActivos.insert(i,value[1])
                ListDataJson += '{"Id":"' + str(DNI) + '","NumProyecto":"' + str(value[0]) + '","NumMaestro":"' + str(value[1]) + '","DiasDeProduccion":' + str(value[2]) + ',"TrabajoPorProgramar":' + str(value[3]) + ',"MargenActual":' + str(value[4]) + ',"PeriodoComparativo":' + str(value[5]) + ',"Empresa":"' + str(value[6]) + '"},' + '\n'
                DNI += 1
    con.commit()
    con.close()
    #here procesing all json in the function tDatJson()
    listMaestrosA = list(set(listMaestrosActivos))
    tDataJason(ListDataJson,valueYear,listMaestrosA,ListDataMargenJson,ListDataJsonCompanys)

print('##################################### End Calculando Throughput ######################################')
