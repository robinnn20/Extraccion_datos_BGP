import pandas as pd
import ipaddress

# Verifica si dos prefijos IPv6 pueden ser agregados en un supernet
def can_aggregate(network1, network2):
    try:
        if network1.prefixlen != network2.prefixlen:
            return False
        supernet = network1.supernet(new_prefix=network1.prefixlen - 1)
        return supernet.overlaps(network2)
    except ValueError:
        return False

# Procesa el archivo de entrada para analizar prefijos IPv6
def analyze_ipv6_prefixes(file_path):
    # Lee el archivo de entrada
    df = pd.read_csv(file_path, delimiter='|', header=None, names=["as_path", "prefix"])

    # Convierte los prefijos a objetos ip_network y organiza por AS de origen
    df['network'] = df['prefix'].apply(lambda x: ipaddress.ip_network(x.strip(), strict=False))
    df['origin_as'] = df['as_path'].apply(lambda x: x.split()[-1])  # Último AS del AS_PATH

    # Cálculo de métricas basadas en prefijos únicos
    unique_networks = df['network'].drop_duplicates()

    # Total de prefijos únicos
    total_prefijos = unique_networks.nunique()
    print(f"Total de prefijos únicos: {total_prefijos}")

    # Cálculo de la longitud promedio de prefijos
    total_prefix_length = sum(network.prefixlen for network in unique_networks)
    average_prefix_length = total_prefix_length / total_prefijos if total_prefijos else 0
    print(f"Average Prefix Length: {average_prefix_length:.2f}")
    
    # Agrupa por AS de origen
    grouped_as = df.groupby('origin_as')

    # Contador de Maximum Aggregateable Prefixes
    max_agg_prefixes_count = 0

    for origin_as, group in grouped_as:
        networks = sorted(group['network'].drop_duplicates().tolist(), key=lambda x: (x.prefixlen, x.network_address))
        combinable = set()
        i = 0

        # Itera sobre los prefijos
        while i < len(networks) - 1:
            network1 = networks[i]
            network2 = networks[i + 1]

            # Si los prefijos son combinables
            if can_aggregate(network1, network2):
                combinable.add((network1, network2))  # Marca los prefijos como combinables
                networks[i + 1] = network1.supernet(new_prefix=network1.prefixlen - 1)  # Actualiza el siguiente prefijo al supernet
                i += 1  # Salta al siguiente par
            else:
                i += 1

        # Contar los prefijos combinables
        max_agg_prefixes_count += len(combinable) * 2  # Cada combinación cuenta como 2 prefijos

    print(f"Maximum Aggregateable Prefixes: {max_agg_prefixes_count}")

    # Cálculo de prefijos no agregables
    non_agg_prefixes_count = total_prefijos - max_agg_prefixes_count
    print(f"Unaggregateables Prefixes: {non_agg_prefixes_count}")

    # Cálculo de saltos del AS_PATH más largo y tamaño promedio de saltos
    df['as_path_length'] = df['as_path'].apply(lambda x: len(x.split()))  # Longitud del AS_PATH
    longest_as_path = df['as_path_length'].max()
    average_as_path_length = df['as_path_length'].mean()

    print(f"Longest AS-Path: {longest_as_path}")
    print(f"Average AS-Path: {average_as_path_length:.2f}")

# Ruta al archivo de entrada
file_path = 'archivo_salida.txt'
analyze_ipv6_prefixes(file_path)
