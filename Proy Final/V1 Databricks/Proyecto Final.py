# Databricks notebook source
#instalamos las librerias necesarias
import pandas as pd
import numpy as np
import matplotlib
from pathlib import Path
import shutil

# COMMAND ----------

#Leemos la carpeta buscando los archivos con el formato de 2022-09*.csv para leer solo septiembre.
count = 0
sales_files_import = pd.DataFrame()

for sales_file in Path('/dbfs/mnt/Lake/LZ/PRD/SD/SKU_ML').glob("2022-09*.csv"):
  file_data = pd.read_csv(sales_file, low_memory = False, header = None, skiprows = 1)
  sales_files_import = sales_files_import.append(file_data, ignore_index = True)
  count +=1
count

# COMMAND ----------

#leemos un archivo con nrows = 0 para leer solo las columnas
df1 = pd.read_csv('/dbfs/mnt/Lake/LZ/PRD/SD/SKU_ML/2022-09-01.csv', low_memory=False, nrows=0)

# COMMAND ----------

#insertamos las columnas en el dataframe completo
sales_files_import.columns = df1.columns

# COMMAND ----------

sales_files_import.head()

# COMMAND ----------

#seleccionamos las columnas que nos interesan
df = sales_files_import[['Create_Date','NumeroPedido','Fecha','Hora','Cliente','Material','Stock','Sugerido','Venta','Peso','Precio','Tipo','Motivo','Region','Sucursal','Ruta']]

# COMMAND ----------

#Signifcado de los valores de Tipo y Motivo
#Tipo: M=Manual, B= Basico, H=Historico, P=Potencial, L=Lanzamiento, BD= Basico destacado, HD=Historico Destacado, PD=Potencial destacado
#Motivo: 01=No tenia en stock, 02=Se elimino manualmente

# COMMAND ----------

#Separamos los dos motivos y un dataframe con ambos
dfF1=df[df["Motivo"] == '01']
dfF2=df[df["Motivo"] == '02']
dfF3=dfF1.append(dfF2, ignore_index=True)


# COMMAND ----------

#mostramos los registros de cada tipo
ax = dfF3["Tipo"].value_counts().plot(kind="bar")
ax.bar_label(ax.containers[0], fmt='%3d')

# COMMAND ----------

#Mostramos los registros de cada motivo
ax = dfF3["Motivo"].value_counts().plot(kind="bar")
ax.bar_label(ax.containers[0], fmt='%3d')

# COMMAND ----------

#usando el dataframe de registros eliminados
#tomamos el material y agregamos un contador
dfMatElimin = dfF2[['Material']]
dfMatElimin['Contador'] = 1
dfMatElimin2 = dfMatElimin.groupby(by=["Material"], as_index=False).sum()


# COMMAND ----------

#Agrupamos por el material para saber cuales son los materiales que mas se eliminan de la aplicacion
ax = dfMatElimin2.sort_values(by=['Contador'], ascending=False).head(10).plot(kind="bar", x='Material', y='Contador')
ax.bar_label(ax.containers[0], fmt='%3d')

# COMMAND ----------

#usando el dataframe de fuera de stock, mostramos los materiales que mas se repiten con ese motivo
dfMatStock = dfF1[['Material']]
dfMatStock['Contador'] = 1
dfMatStock2 = dfMatStock.groupby(by=["Material"], as_index=False).sum()
ax = dfMatStock2.sort_values(by=['Contador'], ascending=False).head(10).plot(kind="bar", x='Material', y='Contador')
ax.bar_label(ax.containers[0], fmt='%3d')


# COMMAND ----------

#calculamos falta de stock
df['StockXSugerido'] = df['Stock']-df['Sugerido']

# COMMAND ----------

#filtramos por los registros donde no se tuvo stock y lo vemos por region
dfNegativo=df[df["StockXSugerido"] <0]

ax = dfNegativo['Region'].value_counts().plot(kind="pie", title="Fuera de stock por region")
pieRegion = dfNegativo['Region'].value_counts()

region = pieRegion.head(1).to_frame().reset_index()
regionMayor = region['index'].head(1)[0]

pieRegion.head(3)

# COMMAND ----------

#tomando la region mas grande, podemos ver las sucurales
print("La region con menos stock es: " + regionMayor)
dfNegativoRegion = dfNegativo[dfNegativo['Region']==regionMayor]


ax = dfNegativoRegion['Sucursal'].value_counts().sort_values(ascending=False).head().plot(kind="pie", title="Fuera de stock por sucursal")
pieReg = dfNegativoRegion['Sucursal'].value_counts()
suc = pieReg.head(1).to_frame().reset_index()
sucursalMayor = suc['index'].head(1)[0]
pieReg.head(3)

# COMMAND ----------

#tomando la sucursal mas grande, vemos las rutas
print("La sucursal de la region: " + regionMayor + "con menos estock es: " + sucursalMayor)
dfNegativoRegionSuc = dfNegativoRegion[dfNegativoRegion['Sucursal']==sucursalMayor]

ax = dfNegativoRegionSuc['Ruta'].value_counts().sort_values(ascending=False).head().plot(kind="pie", title="Fuera de stock por ruta")
pieSuc = dfNegativoRegionSuc['Ruta'].value_counts()
rut = pieSuc.head(1).to_frame().reset_index()
rutaMayor = rut['index'].head(1)[0]
pieSuc.head(3)


# COMMAND ----------

#tomando la ruta mas grande, vemos los productos
print("La Ruta de la region: " + regionMayor + " y sucursal: " + sucursalMayor +  "con menos estock es: " + str(rutaMayor))
dfNegativoRegionSucRuta = dfNegativoRegionSuc[dfNegativoRegionSuc['Ruta']==rutaMayor]
dfNegativoRegionSucRuta.count()
ax = dfNegativoRegionSucRuta['Material'].value_counts().sort_values(ascending=False).head().plot(kind="pie", title="Fuera de stock por ruta")
pieRut = dfNegativoRegionSucRuta['Material'].value_counts()

pieRut.sort_values(ascending=False).head(3)

# COMMAND ----------

#Numero de registros usamos en cada paso
print("Registros crudos")
print(len(sales_files_import.axes[0]))
print("Registros de Tipo 1")
print(len(dfF1.axes[0]))
print("Registros de Tipo 2")
print(len(dfF2.axes[0]))
print("Registros de Tipo 1 y Tipo 2")
print(len(dfF3.axes[0]))
print("Registros con stock negativo")
print(len(dfNegativo.axes[0]))
print("Registros con stock negativo de la region: " + regionMayor + " con mas stock negativo")
print(len(dfNegativoRegion.axes[0]))
print("Registros con stock negativo de la region: " + regionMayor + "  y sucursal: " + sucursalMayor + " con mas stock negativo")
print(len(dfNegativoRegionSuc.axes[0]))
print("Registros con stock negativo de la region: " + regionMayor + ", sucursal: " + sucursalMayor + " y ruta: " + str(rutaMayor) + " con mas stock negativo es")
print(len(dfNegativoRegionSucRuta.axes[0]))

# COMMAND ----------

#Calculamos memoria del dataframe principal
print("Peso del archivo crudo con " + str((len(sales_files_import.axes[0]))) + " registros")
baits =  sales_files_import.memory_usage(index=True, deep=True).sum()
megas = baits/1000000
gigas = megas/1000
print("Peso en bytes")
print(str(baits) + " bytes")
print("Peso en megas")
print(str(megas) + " megas")
print("Peso en Gigas")
print(str(gigas) + " gigas")

