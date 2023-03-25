# -*- coding: utf-8 -*-
"""
El objetivo de ests script es saber cuántos estacionamientos públicos hay por manzana y por alcaldía en la CDMX
"""
import pandas as pd
import numpy as np

ruta=r"C:\Users\Emmanuel\OneDrive - Instituto Politecnico Nacional\ESTANCIAS INDUSTRIALES\CONUEE\RESEARCH\6.- LANDMARKS\ANÁLISIS DE DATOS\ESTACIONAMIENTO-ALCALDÍAS-MANZANAS"
nombre="\Estacionamiento-alcaldias-manzanas.csv"
nombre2="\manzanas totales.csv"
nombre3="\municipios totales.csv"
parkings=pd.read_csv(ruta+nombre) #archivo de estacionamientos por alcaldías y manzanas
man_tot=pd.read_csv(ruta + nombre2) # archivo de manzanas totales en la CDMX
mun_tot=pd.read_csv(ruta + nombre3) # alcaldías totales en la CDMX


#%% En esta primera parte lo que hago es encontrar el número de estacionamientos por municipios 

mun_tot.sort_values("CVE_MUN", inplace=True) #primero ordenamos de menor a mayor las claves municipales del df
cves_tot_mun=list(mun_tot["CVE_MUN"]) #lista de menor a mayor de claves municipales
est_tot_mun=list(range(len(cves_tot_mun))) #lista donde se guardarán el número total de estacionamientos

#La clave estaba en encontrar el índice donde estaba el valor encontrado.

for  cve_est, datos in parkings.groupby("CVE_MUN"): 
    if cve_est in cves_tot_mun:
        indice = cves_tot_mun.index(cve_est)
        est_tot_mun[indice]=len(datos)
    
print(sum(est_tot_mun) == len(parkings)) # vetificamos que el número total de estacionamientos concuerde con los valores de la lista generada

mun_tot["EST_TOT"]=est_tot_mun
#%% Alternativa al código de arriba para obtener el número total de estacionamientos por municipio, 
#en este código solo se ahorra una línea de código y una variable.

mun_tot.sort_values("CVE_MUN", inplace=True) #primero ordenamos de menor a mayor las claves municipales del df
est_tot_mun=list(range(len(mun_tot))) #lista donde se guardarán el número total de estacionamientos

#La clave estaba en encontrar el índice donde estaba el valor encontrado.

for  cve_est, datos in parkings.groupby("CVE_MUN"): 
    if cve_est in pd.unique(mun_tot["CVE_MUN"]):
        indice = pd.unique(mun_tot["CVE_MUN"]).tolist().index(cve_est)
        est_tot_mun[indice]=len(datos)
    
print(sum(est_tot_mun) == len(parkings)) # vetificamos que el número total de estacionamientos concuerde con los valores de la lista generada

mun_tot["EST_TOT"]=est_tot_mun
    
#%%
#Primero ordeno el df de menor a mayor por todas las 4 variables, tanto para el df de estacionamientos como el de manzanas totales

parkings.sort_values(["CVE_MUN","CVE_LOC","AGEB","MANZANA"], inplace=True)
man_tot.sort_values(["CVE_MUN","CVE_LOC","CVE_AGEB","CVE_MZA"], inplace=True)
man_tot.reset_index(inplace=True,drop=True)
parkings.reset_index(inplace=True,drop=True)

#agrupo por estas cuatro variables y obtengo el número de agrupaciones creadas

agrupaciones=parkings.groupby(["CVE_MUN","CVE_LOC","AGEB","MANZANA"]) 

group_values=list(agrupaciones.groups.keys()) # valores de las claves de las manzanas que tienen estacionamientos
num_grup=len(group_values) # Número de manzanas que tienen estacionamientos
print(f"Hay {num_grup} manzanas que tienen estacionamientos")


# Ahora creo una columna de ceros que almacenará el número de estacionamientos por manzana en el dataset de man_tot

man_tot["NUM_EST"]=[0]*len(man_tot)

# 
cont=0 #creo un contador para el número de manzanas encontradas con estacionamientos

for grupo in group_values:  # de todas las manzanas con estacionamientos, ve una por una
    
    n_est=len(agrupaciones.get_group(grupo)) #número total de estacionamientos por manzana
    
    for fila in list(range(len(man_tot))): # ve manzana por manzana en la tabla de manzanas totales
        
        if (
                grupo[0]==man_tot["CVE_MUN"][fila] and        # si la manzana con estacionamientos se encuentra en la tabla de manzanas totales
                grupo[1]==man_tot["CVE_LOC"][fila] and
                grupo[2]==man_tot["CVE_AGEB"][fila] and
                grupo[3]==man_tot["CVE_MZA"][fila]
                ):
            print(f"GOT IT! \t {grupo}\t núm_est = {n_est} \t \t fila= {fila}")
            man_tot["NUM_EST"][fila]=n_est                  # Agrega el número de estacionamientos totales en esa manzana
            cont+=1                                         # registra el número total de manzanas encontradas

if cont==len(group_values): # verifica que todas las manzanas con estacionamientos se encontraron en la tabla total de manzanas 
    print("Todas las manzanas del df de estacionamientos fueron encontradas en el df de manzanas totales")
else:
    print("No todas las manzanas del df de estacionamientos fueron encontradas en el df de manzanas totales")
#%% Encontrando las manzanas que no están en el dataframe de las manzanas totales

MZA_df = man_tot[man_tot["NUM_EST"]!=0] # crea un df con todas las manzanas que tienen estacionamientos en el df de manzanas totales
MZA_df.reset_index(drop=True, inplace=True) 

est_man_A= list(MZA_df.groupby(["CVE_MUN","CVE_LOC","CVE_AGEB","CVE_MZA"]).groups.keys()) #obten las claves de las manzanas con estacionamientos encontradas

todas_manzanas=list(man_tot.groupby(["CVE_MUN","CVE_LOC","CVE_AGEB","CVE_MZA"]).groups.keys()) # obten todas las claves de las manzanas totales

dif=len(group_values)-len(MZA_df) # Encuentra cuántas manzanas con estacionamiento no se encontraron en el df de manzanas totales
print(f"Hay {dif} manzanas que no se encontraron en el dataframe de las manzanas totales")


manzanas_faltantes=[]  # lista para poner todas las manzanas no encontradas
for manzana_estacionamiento in group_values:
    if manzana_estacionamiento not in est_man_A:
        print( manzana_estacionamiento, " Número de estacionamientos en esa manzana: ", len(agrupaciones.get_group(manzana_estacionamiento)))
        manzanas_faltantes.append(manzana_estacionamiento) # guardo las manzanas que no fueron encontradas


# Se verifica que realmente las manzanas no están en el dataframe original de las manzanas totales
for manzana in manzanas_faltantes:
    if manzana not in todas_manzanas:
        print("Se confirma que la manzana ", manzana, " no se encuentra en el total de manzanas")
    
#%% Al final exportamos los DataFrames modificados de los municipios y manzanas de la CDMX
man_tot.to_csv(ruta + "\manzanas totales actualizado.csv", index=False)
mun_tot.to_csv(ruta + "\municipios totales actualizado.csv", index=False)
