#!user/bin/python
from tokens import *
import unicodedata
import pymssql
import pypyodbc as pyodbc
import simplejson as json

#we create a methods for insert in sql and we do calculate the ThroughputCliente
def SqlTroughtPut(sql):
    conn = pymssql.connect(host=hostMSSQL,user=userMSSQL,password=passMSSQL,database=dbMSSQL)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()

def ThroughputClients(Periodo,Empresa,ListDataJsonCompanys):

    MargenXMaestroEmpresa = 0
    TrhoughputRC = 0
    DiasDeProduccion = 0
    TrabajoPorProgramar  = 0

    #for value in dataCompanys['Companys']:
    #    if value['IdEmpresa'] == Empresa:
    #        if value['PeriodoComparativo'] == Periodo:
    #            if int(value['NumMaestro']) == int(NumMaestro):
    #                print('#####Armando#####NumProyecto' + str(value['NumProyecto']) + 'NumMaestro : ' + str(value['NumMaestro']) + 'Empresa :' + str(value['Empresa']) + 'Margen Actual:$' +  str(value['MargenActual']) + 'Periodo Comparativo :' + str(value['PeriodoComparativo']))
    #                MargenXMaestroEmpresa += value['MargenActual']
    #                DiasDeProduccion += value['DiasDeProduccion']
    #                TrabajoPorProgramar += value['TrabajoPorProgramar']
    #                if MargenXMaestroEmpresa > 0:
    #                    x = DiasDeProduccion + TrabajoPorProgramar
    #                    TrhoughputRC = MargenXMaestroEmpresa/x
    #                else:
    #                    TrhoughputRC = 0
    #return TrhoughputRC
#1 .- Get a list of years

listYears = []
i = 0
sql = 'SELECT DISTINCT(ISNULL([PeriodoComparativo],1999)) FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput]'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
for value in cur:
    if value[0] == 1999:
        valuesyears = 0
    else:
        listYears.insert(i,value[0])
con.commit()
con.close()

#2 .-  get a lista of  companys
listCompanys = []
i = 0
sql = 'SELECT [IdEmpresa] FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by [IdEmpresa] asc'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
for value in cur:
    listCompanys.insert(i,value[0])
con.commit()
con.close()
#Here delete duplicate elements
listCompanysA = list(set(listCompanys))

#3 .- We create a json for the calculates
sql = 'SELECT [NumMaestro],[IdEmpresa],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual, ISNULL([PeriodoComparativo],1999) As PeriodoComparativo  FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] order by NumMaestro'
con = pyodbc.connect(constr)
cur = con.cursor()
cur.execute(sql)
ListDataJsonCompanys = ''
ListDataJsonCompanys = '{"Companys":['
DNI = 0
for value in cur:
    if value[0] > 0:
        ListDataJsonCompanys += '{"NumMaestro":' +  str(value[0])  + ',"IdEmpresa":' + str(value[1]) + ',"DiasDeProduccion": ' + str(value[2]) + ',"TrabajoPorProgramar":' + str(value[3]) + ',"MargenActual": ' + str(value[4]) + ',"PeriodoComparativo":' + str(value[5]) + '},' + '\n'
con.commit()
con.close()
temp = len(ListDataJsonCompanys)
ListDataJsonCompanys = ListDataJsonCompanys[:temp - 2]
ListDataJsonCompanys += ']}'
dataCompanys = json.loads(ListDataJsonCompanys)
print('######################################### Begin Calculando Throughput ########################################')
print listYears
for valuePeridos in listYears:
    print ('######## '  + str(valuePeridos))
    for valueIdEmpresa in listCompanysA:
        if valuePeridos == 2017:
            MargenXMaestroEmpresa = 0
            DiasDeProduccion = 0
            TrabajoPorProgramar = 0
            for valueCom in dataCompanys['Companys']:
                if valueIdEmpresa == valueCom['IdEmpresa']:
                    MargenXMaestroEmpresa += valueCom['MargenActual']
                    DiasDeProduccion += valueCom['DiasDeProduccion']
                    TrabajoPorProgramar += valueCom['TrabajoPorProgramar']
            print ('Periodo : ' + str(valuePeridos) + ' Empresa: ' + str(valueIdEmpresa) + 'Dias De Produccion: ' + str(DiasDeProduccion) + ' Trabajo Por Programar :' + str(TrabajoPorProgramar) + ' Margen Actual: $' + str(MargenXMaestroEmpresa))
print('##################################### End Calculando Throughput ######################################')
