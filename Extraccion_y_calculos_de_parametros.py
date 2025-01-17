import pandas as pd
from ipaddress import IPv6Network
#0 -Type: Indica el formato.
#1- Marca de tiempo: Momento de la entrada en formato epoch.
#2 -W/A/B: Indica si se trata de una retirada (withdrawn), un anuncio (announcement) o una tabla de enrutamiento (routing table).
#3- Peer IP: Direcci´on IP del monitor.
#4 -Peer ASN: N´umero de Sistema Aut´onomo (ASN ) del monitor.
#5- Prefijo: Bloque de direcciones IP anunciado
#6- ASPath: Lista de AS atravesados para alcanzar el destino.
#7- Protocolo de origen: Usualmente IGP.
#8-Siguiente Hop: Direcci´on IP del siguiente salto.
#9- LocalPref: Preferencia local asignada a la ruta.
#10- MED: Discriminador de salida m´ultiple.
#11-Cadenas comunitarias: Valores comunitarios asociados a la ruta.
#12- Agregador at´omico: Indicador de rutas agregadas.
#13- Agregador: Informaci´on adicional sobre el agregador.

# Se hace uso de la funcion de pandas para filtrar por columnas los datos tabulados separadas por "|"
data = pd.read_csv('example.txt', sep='|', header=None)

# Filtrar solo prefijos IPv6 en base a un caracter :
data_ipv6 = data[data[5].str.contains(':')].copy()

# Nos aseguramos que el total de prefijos IPv6 sea único en la columna 5
total_prefijos_ipv6 = data_ipv6[5].nunique()
print(f"Total de prefijos IPv6 únicos: {total_prefijos_ipv6}")

# Función para verificar si dos prefijos IPv6 pueden ser agregados
def son_agregables(red1, red2):

    #La función asegura que los prefijos sean sumarizables verificando que:
     #Cubran la misma cantidad de direcciones (misma longitud).
     #Sean adyacentes sin solapamiento ni espacios entre ellos.
    return red1.prefixlen == red2.prefixlen and (red1.network_address + red1.num_addresses == red2.network_address)

# Función para contar prefijos agregables y no agregables 
def contar_agregables_y_no_agregables(grupo):
   #se crea una lista o grupo de  prefijos para cada ASN
    prefijos = grupo.tolist()
    redes = [IPv6Network(p) for p in prefijos]  # Convertir prefijos a objetos IPv6Network
    redes.sort()  # Ordenar por dirección base y longitud
    
   #se declaran 2 contadores para prefijos agregables y no agregables
    agregables = 0
    no_agregables = 0
    i = 0

    while i < len(redes) - 1:
        # Si los prefijos consecutivos son agregables, los contamos como agregables
        if son_agregables(redes[i], redes[i + 1]):
            agregables += 1
            i += 2  # Saltamos el siguiente prefijo, ya que ambos se agregan
        else:
            no_agregables += 1
            i += 1  # Avanzamos al siguiente prefijo

    # Si queda un prefijo sin contar, es no agregable
    if i < len(redes):
        no_agregables += 1

    return agregables, no_agregables

# Agrupar por ASN (columna 4) y calcular los prefijos agregables y no agregables segun los propios prefijos anunciados en la columna 5
agregables_y_no_agregables_por_asn = (
    data_ipv6.groupby(4)[5]
    .apply(contar_agregables_y_no_agregables)
)

# Obtener la suma total de prefijos agregables y no agregables
total_agregables = agregables_y_no_agregables_por_asn.apply(lambda x: x[0]).sum()
#total_no_agregables = agregables_y_no_agregables_por_asn.apply(lambda x: x[1]).sum()

# Calcular los no agregables como la diferencia entre el total de prefijos y los agregables
no_agregables_calculados = total_prefijos_ipv6 - total_agregables

print(f"Total de prefijos IPv6 únicos: {total_prefijos_ipv6}")
print(f"Total de prefijos agregables: {total_agregables}")
print(f"Total de prefijos no agregables (calculados): {no_agregables_calculados}")

# Se utiliza un espacio simple como caracer para identificar el aspath mas largo y su promedio
data_ipv6['as_path_largo'] = data_ipv6[6].apply(lambda x: len(x.split(' ')))
as_path_mas_largo = data_ipv6['as_path_largo'].max()
as_path_promedio = data_ipv6['as_path_largo'].mean()
print(f"AS-Path más largo: {as_path_mas_largo}")
print(f"AS-Path promedio: {as_path_promedio:.2f}")
