import pandas as pd
from ipaddress import IPv6Network

# Se hace uso de la funcion de pandas para filtrar por columnas los datos tabulados separadas por "|"
data = pd.read_csv('datos_rib.txt', sep='|', header=None)

# Filtrar solo prefijos IPv6 en base a un caracter :
data_ipv6 = data[data[5].str.contains(':')].copy()

# Función para calcular si dos prefijos son contiguos, recibe 2 objetos (red1 y red2) de tipo IPv6Network como parametros
def son_contiguos(red1, red2):
  #La función asegura que los prefijos sean sumarizables verificando que:
     #Cubran la misma cantidad de direcciones (misma longitud).
     #Sean adyacentes sin solapamiento ni espacios entre ellos.
    return red1.prefixlen == red2.prefixlen and red1.network_address + red1.num_addresses == red2.network_address

# Función para sumarizar los prefijos dentro de un grupo de ASN
def sumarizar_prefijos(prefijos):

#    Sumariza los prefijos dentro de un grupo dado.
 #   Los prefijos se consideran sumarizables si son contiguos y tienen la misma longitud.
 #   
    redes = [IPv6Network(p) for p in prefijos]  # Convertir prefijos a objetos IPv6Network
    redes.sort()  # Ordenar por dirección base

    sumarizados = []
    i = 0
    while i < len(redes):
        red_actual = redes[i]
        # Sumarizar con el siguiente prefijo de manera que se comparen todos los prefijos si es que  son contiguos, haciendo uso de la funcion que verifica que lo son.
        while i + 1 < len(redes) and son_contiguos(red_actual, redes[i + 1]):
            red_actual = IPv6Network(f"{red_actual.network_address}/{red_actual.prefixlen}")
            i += 1
        sumarizados.append(red_actual)
        i += 1

    return sumarizados

# Función para contar los prefijos no sumarizables por ASN
def contar_no_sumarizables(grupo):
    prefijos = grupo.tolist()
    sumarizados = sumarizar_prefijos(prefijos)

    # Los prefijos no sumarizables son los que no se han podido agregar
    no_sumarizables = len(prefijos) - len(sumarizados)
    return no_sumarizables

# Agrupar por AS (columna 4) y calcular los prefijos no sumarizables de cada AS
no_sumarizables_por_asn = (
    data_ipv6.groupby(4)[5]
    .apply(contar_no_sumarizables)
)

# Total de prefijos únicos
total_prefijos = data_ipv6[5].nunique()
print(f"Total de prefijos: {total_prefijos}")

# Longitud promedio de los prefijos únicos
prefijos_unicos = data_ipv6[5].unique()
longitudes_prefijos = [IPv6Network(p).prefixlen for p in prefijos_unicos]
longitud_promedio_prefijos = sum(longitudes_prefijos) / len(longitudes_prefijos)
print(f"Longitud promedio de los prefijos únicos: {longitud_promedio_prefijos:.2f}")

# Total de prefijos no sumarizables
num_prefijos_no_agregables = no_sumarizables_por_asn.sum()
print(f"Prefijos no sumarizables: {num_prefijos_no_agregables}")


# Prefijos que son agregables segun los criterios de sumarizacion y contiguidad
num_prefijos_agregables = total_prefijos - num_prefijos_no_agregables
print(f"Máximo número de prefijos agregables: {num_prefijos_agregables}")

# AS-Path más largo y promedio
data_ipv6['as_path_largo'] = data_ipv6[6].apply(lambda x: len(x.split(' ')))
as_path_mas_largo = data_ipv6['as_path_largo'].max()
as_path_promedio = data_ipv6['as_path_largo'].mean()
print(f"AS-Path más largo: {as_path_mas_largo}")
print(f"AS-Path promedio: {as_path_promedio:.2f}")

