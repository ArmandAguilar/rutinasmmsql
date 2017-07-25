#!user/bin/python
from tokens import *
import unicodedata
import pymssql
import pypyodbc as pyodbc
import simplejson as json


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
