import pandas as pd
from ipaddress import IPv6Network

# Leer el archivo filtrado
data = pd.read_csv('example.txt', sep='|', header=None)

# Filtrar solo prefijos IPv6 (se hace en la columna 5)
data_ipv6 = data[data[5].str.contains(':')].copy()

# El total de prefijos IPv6 únicos en la columna 5
total_prefijos_ipv6 = data_ipv6[5].nunique()
print(f"Total de prefijos IPv6 únicos: {total_prefijos_ipv6}")

# Función para verificar si dos prefijos IPv6 pueden ser agregados
def son_agregables(red1, red2):
    """
    Verifica si dos prefijos IPv6 pueden ser agregados en uno más grande.
    """
    return red1.prefixlen == red2.prefixlen and (red1.network_address + red1.num_addresses == red2.network_address)

# Función para contar prefijos agregables y no agregables dentro de un grupo de ASN
def contar_agregables_y_no_agregables(grupo):
    """
    Calcula la cantidad de prefijos agregables y no agregables por ASN.
    """
    prefijos = grupo.tolist()
    redes = [IPv6Network(p) for p in prefijos]  # Convertir prefijos a objetos IPv6Network
    redes.sort()  # Ordenar por dirección base y longitud

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

# Agrupar por ASN (columna 4) y calcular los prefijos agregables y no agregables
agregables_y_no_agregables_por_asn = (
    data_ipv6.groupby(4)[5]
    .apply(contar_agregables_y_no_agregables)
)

# Obtener la suma total de prefijos agregables y no agregables
total_agregables = agregables_y_no_agregables_por_asn.apply(lambda x: x[0]).sum()
total_no_agregables = agregables_y_no_agregables_por_asn.apply(lambda x: x[1]).sum()

# Calcular los no agregables como la diferencia entre el total de prefijos y los agregables
no_agregables_calculados = total_prefijos_ipv6 - total_agregables

print(f"Total de prefijos IPv6 únicos: {total_prefijos_ipv6}")
print(f"Total de prefijos agregables: {total_agregables}")
print(f"Total de prefijos no agregables (calculados): {no_agregables_calculados}")

# AS-Path más largo y promedio
data_ipv6['as_path_largo'] = data_ipv6[6].apply(lambda x: len(x.split(' ')))
as_path_mas_largo = data_ipv6['as_path_largo'].max()
as_path_promedio = data_ipv6['as_path_largo'].mean()
print(f"AS-Path más largo: {as_path_mas_largo}")
print(f"AS-Path promedio: {as_path_promedio:.2f}")
