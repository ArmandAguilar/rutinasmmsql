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
def DtaJsonCom(periodo):
    sql = 'SELECT [NumMaestro],[IdEmpresa],[Dias de produccion] As DiasDeProduccion ,[Trabajo por programar] As TrabajoPorProgramar,[Margen Actual] As MargenActual, ISNULL([PeriodoComparativo],1999) As PeriodoComparativo,[Empresa]  FROM [SAP].[dbo].[RV-ESTADOPROYECTOS-AA-Throughput] where [PeriodoComparativo] = \'' + str(periodo) + '\''
    con = pyodbc.connect(constr)
    cur = con.cursor()
    cur.execute(sql)
    ListDataJsonCompanys = ''
    ListDataJsonCompanys = '{"Companys":['
    DNI = 0
    for value in cur:
        if value[0] > 0:
            ListDataJsonCompanys += '{"NumMaestro":' +  str(value[0])  + ',"IdEmpresa":' + str(value[1]) + ',"DiasDeProduccion": ' + str(value[2]) + ',"TrabajoPorProgramar":' + str(value[3]) + ',"MargenActual": ' + str(value[4]) + ',"PeriodoComparativo":' + str(value[5]) + ',"Empresa":"' + str(value[6]) + '"},' + '\n'
    con.commit()
    con.close()
    temp = len(ListDataJsonCompanys)
    ListDataJsonCompanys = ListDataJsonCompanys[:temp - 2]
    ListDataJsonCompanys += ']}'
    return ListDataJsonCompanys

#dataCompanys = json.loads(ListDataJsonCompanys)
print('########################################## Begin Calculando Throughput #########################################')
sql = 'Delete FROM [SAP].[dbo].[ThrougputCliente]'
SqlTroughtPut(sql)

print (listCompanysA)
for valuePeridos in listYears:
    print ('######## '  + str(valuePeridos))
    for valueIdEmpresa in listCompanysA:
        print ('====> '  + str(valueIdEmpresa))

#for valuePeridos in listYears:
#    print ('######## '  + str(valuePeridos))
#    dataCompanys0 = json.loads(DtaJsonCom(str(valuePeridos)))
#    for valueIdEmpresa in listCompanysA:
#        MargenXMaestroEmpresa = 0
#        DiasDeProduccion = 0
#        TrabajoPorProgramar = 0
#        Emp = ''
#        for valueCom in dataCompanys0['Companys']:
#            if valueIdEmpresa == valueCom['IdEmpresa']:
#                if valuePeridos == valueCom['PeriodoComparativo']:
                    #pass
                    #print ('Empresa in JSNO : ' + str(valueIdEmpresa) + ' == ' + 'Empresa in List : ' + str(valueIdEmpresa))
#                    MargenXMaestroEmpresa += valueCom['MargenActual']
#                    DiasDeProduccion += valueCom['DiasDeProduccion']
#                    TrabajoPorProgramar += valueCom['TrabajoPorProgramar']
#    x = DiasDeProduccion + TrabajoPorProgramar
#    if x > 0:
#        TrhoughputRC = MargenXMaestroEmpresa/x
#    else:
#        TrhoughputRC = 0
    #print ('Periodo : ' + str(valuePeridos) + ' Empresa: ' + str(Emp) + 'Dias De Produccion: ' + str(DiasDeProduccion) + ' Trabajo Por Programar :' + str(TrabajoPorProgramar) + ' Margen Actual: $' + str(MargenXMaestroEmpresa) + ' TrhoughputCliente $ ' + str(TrhoughputRC))
#    sql = 'INSERT INTO [SAP].[dbo].[ThrougputCliente] VALUES (\'' + str(valueCom['IdEmpresa']) + '\',\'' + str(MargenXMaestroEmpresa) + '\',\'' + str(TrhoughputRC) + '\',\'' + str(valuePeridos) + '\')'
#    print(sql)
        #print ('Periodo : ' + str(valuePeridos) + ' Empresa: ' + str(Emp) + 'Dias De Produccion: ' + str(DiasDeProduccion) + ' Trabajo Por Programar :' + str(TrabajoPorProgramar) + ' Margen Actual: $' + str(MargenXMaestroEmpresa) + ' TrhoughputCliente $ ' + str(TrhoughputRC))

print('##################################### End Calculando Throughput ######################################')
