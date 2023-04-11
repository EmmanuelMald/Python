# -*- coding: utf-8 -*-
"""
El objetivo de ests script es saber cuántos estacionamientos públicos hay por manzana y por alcaldía en la CDMX
"""
# %%
import pandas as pd


ruta = r"C:\Users\Emmanuel\OneDrive - Instituto Politecnico Nacional\ESTANCIAS INDUSTRIALES\CONUEE\RESEARCH\6.- LANDMARKS\ANÁLISIS DE DATOS\ESTACIONAMIENTO-ALCALDÍAS-MANZANAS"
nombre = "\Estacionamiento-alcaldias-manzanas.csv"
nombre2 = "\manzanas totales.csv"
nombre3 = "\municipios totales.csv"
# archivo de estacionamientos por alcaldías y manzanas
parking = pd.read_csv(ruta+nombre)
man_tot = pd.read_csv(ruta + nombre2)  # archivo de manzanas totales en la CDMX
mun_tot = pd.read_csv(ruta + nombre3)  # alcaldías totales en la CDMX


# %% En esta primera parte lo que hago es encontrar el número de estacionamientos por municipios

# primero ordenamos de menor a mayor las claves municipales del df
mun_tot.sort_values("CVE_MUN", inplace=True)
# lista de menor a mayor de claves municipales
cves_tot_mun = list(mun_tot["CVE_MUN"])
# lista donde se guardarán el número total de estacionamientos
est_tot_mun = list(range(len(cves_tot_mun)))

# La clave estaba en encontrar el índice donde estaba el valor encontrado.

for cve_est, datos in parking.groupby("CVE_MUN"):
    if cve_est in cves_tot_mun:
        indice = cves_tot_mun.index(cve_est)
        est_tot_mun[indice] = len(datos)

# vetificamos que el número total de estacionamientos concuerde con los valores de la lista generada
print(sum(est_tot_mun) == len(parking))

mun_tot["EST_TOT"] = est_tot_mun
# %% Alternativa al código de arriba para obtener el número total de estacionamientos por municipio,
# en este código solo se ahorra una línea de código y una variable.

# primero ordenamos de menor a mayor las claves municipales del df
mun_tot.sort_values("CVE_MUN", inplace=True)
# lista donde se guardarán el número total de estacionamientos
est_tot_mun = list(range(len(mun_tot)))

# La clave estaba en encontrar el índice donde estaba el valor encontrado.

for cve_est, datos in parking.groupby("CVE_MUN"):
    if cve_est in pd.unique(mun_tot["CVE_MUN"]):
        indice = pd.unique(mun_tot["CVE_MUN"]).tolist().index(cve_est)
        est_tot_mun[indice]= len(datos)

# vetificamos que el número total de estacionamientos concuerde con los valores de la lista generada
print(sum(est_tot_mun) == len(parking))

mun_tot["EST_TOT"] = est_tot_mun

# %%
# Primero ordeno el df de menor a mayor por todas las 4 variables, tanto para el df de estacionamientos como el de manzanas totales

parking.sort_values(["CVE_MUN", "CVE_LOC", "AGEB", "MANZANA"], inplace=True)
man_tot.sort_values(
    ["CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"], inplace=True)
man_tot.reset_index(inplace=True, drop=True)
parking.reset_index(inplace=True, drop=True)

# agrupo por estas cuatro variables y obtengo el número de agrupaciones creadas

agrupaciones = parking.groupby(["CVE_MUN", "CVE_LOC", "AGEB", "MANZANA"])

# valores de las claves de las manzanas que tienen estacionamientos
group_values = list(agrupaciones.groups.keys())
num_grup = len(group_values)  # Número de manzanas que tienen estacionamientos
print(f"Hay {num_grup} manzanas que tienen estacionamientos")


# Ahora creo una columna de ceros que almacenará el número de estacionamientos por manzana en el dataset de man_tot

man_tot["NUM_EST"] = [0]*len(man_tot)

#
cont = 0  # creo un contador para el número de manzanas encontradas con estacionamientos

for grupo in group_values:  # de todas las manzanas con estacionamientos, ve una por una

    # número total de estacionamientos por manzana
    n_est = len(agrupaciones.get_group(grupo))

    # ve manzana por manzana en la tabla de manzanas totales
    for fila in list(range(len(man_tot))):

        if (
                # si la manzana con estacionamientos se encuentra en la tabla de manzanas totales
                grupo[0] == man_tot["CVE_MUN"][fila] and
                grupo[1] == man_tot["CVE_LOC"][fila] and
                grupo[2] == man_tot["CVE_AGEB"][fila] and
                grupo[3] == man_tot["CVE_MZA"][fila]
        ):
            print(f"GOT IT! \t {grupo}\t núm_est = {n_est} \t \t fila= {fila}")
            # Agrega el número de estacionamientos totales en esa manzana
            man_tot["NUM_EST"][fila] = n_est
            # registra el número total de manzanas encontradas
            cont += 1

# verifica que todas las manzanas con estacionamientos se encontraron en la tabla total de manzanas
if cont == len(group_values):
    print("Todas las manzanas del df de estacionamientos fueron encontradas en el df de manzanas totales")
else:
    print("No todas las manzanas del df de estacionamientos fueron encontradas en el df de manzanas totales")
# %% ALTERNATIVA PARA ENCONTRAR LAS MANZANAS
parking.sort_values(["CVE_MUN", "CVE_LOC", "AGEB", "MANZANA"], inplace=True)
parking.reset_index(inplace=True, drop=True)
man_tot.sort_values(
    ["CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"], inplace=True)
man_tot.reset_index(inplace=True, drop=True)

group_manz_est = parking.groupby(["CVE_MUN", "CVE_LOC", "AGEB", "MANZANA"])
group_manz_tot = man_tot.groupby(["CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"])
manz = list(group_manz_tot.groups.keys())
manz_est = list(group_manz_est.groups.keys())

cont = 0
man_tot["NUM_EST"] = [0]*len(man_tot)
for manzana in manz_est:
    if manzana in manz:
        indice = manz.index(manzana)
        man_tot["NUM_EST"][indice] = len(group_manz_est.get_group(manzana))
        cont += 1
print(cont)

# %% Encontrando las manzanas que no están en el dataframe de las manzanas totales

# crea un df con todas las manzanas que tienen estacionamientos en el df de manzanas totales
MZA_df = man_tot[man_tot["NUM_EST"] != 0]
MZA_df.reset_index(drop=True, inplace=True)

# obten las claves de las manzanas con estacionamientos encontradas
est_man_A = list(MZA_df.groupby(
    ["CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"]).groups.keys())

# obten todas las claves de las manzanas totales
todas_manzanas = list(man_tot.groupby(
    ["CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"]).groups.keys())

# Encuentra cuántas manzanas con estacionamiento no se encontraron en el df de manzanas totales
dif = len(group_values)-len(MZA_df)
print(f"Hay {dif} manzanas que no se encontraron en el dataframe de las manzanas totales")


manzanas_faltantes = []  # lista para poner todas las manzanas no encontradas
for manzana_estacionamiento in group_values:
    if manzana_estacionamiento not in est_man_A:
        print(manzana_estacionamiento, " Número de estacionamientos en esa manzana: ", len(
            agrupaciones.get_group(manzana_estacionamiento)))
        # guardo las manzanas que no fueron encontradas
        manzanas_faltantes.append(manzana_estacionamiento)


# Se verifica que realmente las manzanas no están en el dataframe original de las manzanas totales
for manzana in manzanas_faltantes:
    if manzana not in todas_manzanas:
        print("Se confirma que la manzana ", manzana,
              " no se encuentra en el total de manzanas")

# %% Al final exportamos los DataFrames modificados de los municipios y manzanas de la CDMX
man_tot.to_csv(ruta + "\manzanas totales actualizado.csv", index=False)
mun_tot.to_csv(ruta + "\municipios totales actualizado.csv", index=False)
