import pandas as pd
import ipaddress

# Verifica si dos prefijos IPv6 pueden ser agregados en un supernet
def can_aggregate(network1, network2):
    try:
        #Primero, revisa si las dos redes tienen el mismo tamaño de prefijo
        if network1.prefixlen != network2.prefixlen:
           # Si no tienen el mismo tamaño, no pueden ser combinadas
            return False
        # Si los dos prefijos son iguales, intenta crear una superred (una red que abarque ambas) con un prefijo un poco más grande, reduciendo en uno la longitud del prefijo de network1
        supernet = network1.supernet(new_prefix=network1.prefixlen - 1)
       # verifica si la nueva superred (con prefijo más grande) se solapa con network2. Si se solapan, significa que las dos redes pueden ser agregadas en una superred, y devuelve True. Si no se solapan, devuelve False
        return supernet.overlaps(network2)
    except ValueError:
        return False

# Procesa el archivo de entrada para analizar prefijos IPv6
def analyze_ipv6_prefixes(file_path):
    # Lee el archivo de entrada
    df = pd.read_csv(file_path, delimiter='|', header=None, names=["prefix", "as_path"])

    # Convierte los prefijos a objetos ip_network y organiza por AS de origen
    df['network'] = df['prefix'].apply(lambda x: ipaddress.ip_network(x.strip(), strict=False))
    df['origin_as'] = df['as_path'].apply(lambda x: x.split()[-1])  # Último AS del AS_PATH

    # Calculo de métricas basadas en prefijos únicos
    unique_networks = df['network'].drop_duplicates()

    # Total de prefijos únicos
    total_prefijos = unique_networks.nunique()
    print(f"Total de prefijos únicos: {total_prefijos}")

    # Cálculo de la longitud promedio de prefijos


    #Aquí se calcula la suma de las longitudes de los prefijos (prefixlen) de todas las redes en unique_networks.
    #Por ejemplo, si unique_networks contiene redes con prefijos /24, /32 y /16, esta línea sumará 24 + 32 + 16.
    total_prefix_length = sum(network.prefixlen for network in unique_networks)

    #Se divide la suma total de los prefijos (total_prefix_length) por el número total de redes unicas (total_prefijos) para calcular el promedio.
    #Si no hay redes (es decir, total_prefijos es 0), evita la división por cero y asigna el valor 0 como promedio.
    average_prefix_length = total_prefix_length / total_prefijos if total_prefijos else 0
    print(f"Average Prefix Length: {average_prefix_length:.2f}")
    
    # Agrupa por AS de origen
    grouped_as = df.groupby('origin_as')

    # Contador de Maximum Aggregateable Prefixes
    max_agg_prefixes_count = 0


    #Recorre cada grupo de redes (agrupadas por algún criterio, como su sistema autónomo o origin_as).
    #group es una colección de redes asociadas a un mismo origen (origin_as).
    for origin_as, group in grouped_as:

        #Elimina redes duplicadas y las convierte en una lista ordenada:
        #Primero, por la longitud del prefijo (prefixlen), de menor a mayor (más específicas primero).
        #Luego, por la dirección de red (network_address).
        networks = sorted(group['network'].drop_duplicates().tolist(), key=lambda x: (x.prefixlen, x.network_address))
        #combinable: Un conjunto para almacenar pares de redes que se pueden combinar.
        combinable = set()
        #Se declara un indice i para recorrer la lista de redes.
        i = 0

        # Itera sobre los prefijos
        while i < len(networks) - 1:
            
            #network1 es la red actual y network2 es la siguiente en la lista
            
            network1 = networks[i]
            network2 = networks[i + 1]
            #Llama a la función can_aggregate para determinar si las dos redes pueden ser combinadas en una superred.
            if can_aggregate(network1, network2):
                combinable.add((network1, network2))  # Marca los prefijos como combinables
                networks[i + 1] = network1.supernet(new_prefix=network1.prefixlen - 1)  # Actualiza el siguiente prefijo al supernet
                i += 1  # Salta al siguiente par
            else:
                i += 1

        # Contar los prefijos combinables
        max_agg_prefixes_count += len(combinable) * 2  # Cada combinación cuenta como 2 prefijos dado que se involucran ambos entonces esa combinacion es valida

    print(f"Maximum Aggregateable Prefixes: {max_agg_prefixes_count}")

    # Cálculo de prefijos no agregables
    non_agg_prefixes_count = total_prefijos - max_agg_prefixes_count
    print(f"Unaggregateables Prefixes: {non_agg_prefixes_count}")

    # Cálculo de saltos del AS_PATH más largo y tamaño promedio de saltos 
    #x.split(): Divide la cadena en una lista de números (sistemas autónomos), usando espacios como separador.
    #len(...): Cuenta cuántos elementos tiene la lista, que equivale a la cantidad de saltos en la ruta.
    df['as_path_length'] = df['as_path'].apply(lambda x: len(x.split()))  # Longitud del AS_PATH
    longest_as_path = df['as_path_length'].max()
    average_as_path_length = df['as_path_length'].mean()

    print(f"Longest AS-Path: {longest_as_path}")
    print(f"Average AS-Path: {average_as_path_length:.2f}")

# Ruta al archivo de entrada
file_path = 'archivo_salida.txt'
analyze_ipv6_prefixes(file_path)
