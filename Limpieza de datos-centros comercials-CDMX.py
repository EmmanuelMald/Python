# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 11:43:14 2023

@author: Emmanuel
"""

import pandas as pd
import numpy as np

ruta="C:\\Users\\Emmanuel\\OneDrive - Instituto Politecnico Nacional\\ARREGLOS\\ESTANCIAS INDUSTRIALES\\CONUEE\\RESEARCH\\6.- LANDMARKS\\Centros y plazas comerciales\\cypc"
nombre="\\Plazas y centros comerciales.csv"
archivo=ruta+nombre
data=pd.read_csv(archivo)


#%%  ELIMICIÓN DE DATOS VACIOS
"""
    Lo que quiero hacer en esta parte es saber si hay vaLores vacíos en la columna TIPO
    
    Si los hay, devuélveme los valores de esos índices y elimínalos del dataframe

"""
print("El número de filas que no tienen nada en TIPO son: ", pd.isnull(data["TIPO"]).values.ravel().sum()) # Me da el número de filas vacías

valores=pd.isnull(data["TIPO"])#esta es un tipo series, tiene dos valores, TRUE (valor vacío), FALSE (no es vacío)
valores=np.array(valores) #convierto el tipo series en array de np
vacios_index=np.where(valores==True)[0] #Devuelve una tupla con los índices donde se encuentra el valor TRUE
# Le pongo el [0] porque devuelve dos posiciones, en la primera está la lista de los índices, y en la segunda posición quien sabe qué sea XD

data.drop(inplace =True,index=vacios_index,axis=0) #Inplace: True (borra en el mismo df), index= lista de índices a borrar, axis : 0-filas, 1-columnas

print("Después de eliminar las filas que no tienen valor en la columna TIPO, verifico de nuevo y hay ", pd.isnull(data["TIPO"]).ravel().sum(), "valores vacíos")
#%% REVISIÓN DIVISIONES DE LOS TIPOS DE CENTROS COMERCIALES


def clascols(name_column):
    tipos=data.groupby(name_column) # agrupo los datos conforme el número de clases generadas en la variable tipo
    print("Se tienen ", len(tipos), " clasificaciones, donde cada una de estas tiene el nombre de: ")
    for tipo, datos in tipos: 
        print(tipo,"          ",len(datos))
    print("\n\n\n")
    return 0

clascols("TIPO")
# Después paso a reemplazar los valores similares a uno solo. De manera que solo tenga seis

#LAS PRIMERAS SON PARA ARREGLAR LOS VALORES DE CENTRO Y PLAZA COMERCIAL 

data["TIPO"].replace("CENTO Y PLAZA COMERCIAL","CENTRO Y PLAZA COMERCIAL", inplace=True) # df.replace(valor a reemplazar, valor reemplazado)
data["TIPO"].replace("CENTRO O PLAZA COMERCIAL","CENTRO Y PLAZA COMERCIAL", inplace=True) 
data["TIPO"].replace("CENTRO Y  PLAZA COMERCIAL","CENTRO Y PLAZA COMERCIAL", inplace=True) 
data["TIPO"].replace("CENTRO Y PLAZA COMERCUAL","CENTRO Y PLAZA COMERCIAL", inplace=True)
data["TIPO"].replace("CENTRO Y PLAZA COMERICIAL","CENTRO Y PLAZA COMERCIAL", inplace=True)
data["TIPO"].replace("CENTRO YPLAZA COMERCIAL","CENTRO Y PLAZA COMERCIAL", inplace=True)
data["TIPO"].replace("Centro y Plaza Comercial","CENTRO Y PLAZA COMERCIAL", inplace=True)
data["TIPO"].replace("PLAZA Y CENTRO COMERCIAL","CENTRO Y PLAZA COMERCIAL", inplace=True)

# ARREGLAMOS LAS TIENDAS DE AUTOSERVICIO

data["TIPO"].replace("TIENDA DE AUTO SERVICIO","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDA DE AUTO SERVICIO_","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDA DE AUTOSERVICIO","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDA DE AUTOSERVICIO_","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDA DE AUTOSERVICIO_1","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDAS DE AUTO SERVICIO","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDAS DE AUTO SERVICIO_","TIENDAS DE AUTOSERVICIO", inplace=True)
data["TIPO"].replace("TIENDAS DE AUTOSERVICIO_1","TIENDAS DE AUTOSERVICIO", inplace=True)

# ARREGLAMOS LAS TIENDAS DEPARTAMENTALES

data["TIPO"].replace("TIENDA DEPARTAMENTAL_1", "TIENDAS DEPARTAMENTALES", inplace=True)
data["TIPO"].replace("TIENDAS DEPARTAMENTALES_1", "TIENDAS DEPARTAMENTALES", inplace=True)

clascols("TIPO")
#%%
"""
Según la exploración de datos, en realidad la clasificación de estas debe ser: 
    TIENDAS DE AUTOSERVICIO
    TIENDAS DEPARTAMENTALES
    CENTRO Y PLAZA COMERCIAL
    CENTRO COMERCIAL
    PLAZA COMERCIAL
    MIXTO
    """
#%% Aquí se hace una sustitución de valores vacíos por Desconocido
clascols("INSTITUCIO")
print("El número de filas que no tienen nada en INSTITUCIO son: ", pd.isnull(data["INSTITUCIO"]).values.ravel().sum()) # Me da el número de filas vacías
data.fillna("Desconocido",inplace=True)




#%% Se convierte a csv para luego importarlo a Mapa Digital

data.to_csv("C:\\Users\\Emmanuel\\OneDrive - Instituto Politecnico Nacional\\ARREGLOS\\ESTANCIAS INDUSTRIALES\\CONUEE\\RESEARCH\\6.- LANDMARKS\\Centros y plazas comerciales\\cypc\\plazas y centros comerciales.csv", index=False)