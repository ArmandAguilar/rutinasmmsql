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
