import asyncio
import ipaddress
import pandas as pd
from collections import defaultdict

# Definimos el trie de Patricia
class PatriciaTrieNode:
    def __init__(self):
        self.children = {}
        self.network = None  # Guarda la red si el nodo es un prefijo válido
        self.is_aggregated = False  # Marca si el prefijo ya fue combinado

class PatriciaTrie:
    def __init__(self):
        self.root = PatriciaTrieNode()

    def insert(self, network):
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)[:network.prefixlen]
        
        for bit in prefix_str:
            if bit not in node.children:
                node.children[bit] = PatriciaTrieNode()
            node = node.children[bit]
        
        node.network = network

    def find_supernet_or_contiguous(self, network):
        """Busca si el prefijo dado pertenece a una supernet o es contiguo a otro prefijo."""
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)
        supernet_candidate = None

        # Busca en el trie mientras sea posible
        for bit in prefix_str:
            if bit in node.children:
                node = node.children[bit]
                if node.network and node.network.prefixlen < network.prefixlen:
                    supernet_candidate = node.network
            else:
                break

        if node and node.network:
            next_prefix = network.network_address + (1 << (128 - network.prefixlen))
            if node.network.network_address == next_prefix:
                return node.network

        return supernet_candidate

    def mark_as_aggregated(self, network):
        """Marca un prefijo como agregado."""
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)[:network.prefixlen]
        for bit in prefix_str:
            node = node.children[bit]
        if node.network == network:
            node.is_aggregated = True

# Almacena ASNs ya consultados para evitar consultas repetidas
asn_cache = {}

# Limitar el número de consultas simultáneas (por ejemplo, 10 consultas concurrentes)
SEMAPHORE_LIMIT = 200
semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

# Función para limpiar el AS Path
def clean_as_path(as_path):
    """Limpia el as_path de caracteres innecesarios como `{}`, `,`."""
    cleaned = as_path.replace("{", "").replace("}", "").replace(",", " ").split()
    return cleaned

# Verifica si un ASN está registrado usando WHOIS de forma asíncrona
async def is_asn_registered(asn):
    if asn in asn_cache:
        return asn_cache[asn]

    async with semaphore:
        try:
            process = await asyncio.create_subprocess_exec(
                "whois", f"AS{asn}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            rir_keywords = ['ARIN', 'RIPE', 'APNIC', 'LACNIC', 'AFRINIC']
            registered = any(keyword in stdout.decode() for keyword in rir_keywords)

            # Cacheamos el resultado para evitar consultas repetidas
            asn_cache[asn] = registered
            return registered
        except Exception as e:
            print(f"Error al consultar ASN {asn}: {e}")
            asn_cache[asn] = False
            return False

# Función para analizar los prefijos IPv6
async def analyze_ipv6_prefixes(file_path):
    df = pd.read_csv(file_path, delimiter='|', header=None, names=["prefix", "as_path"])
    df['network'] = df['prefix'].apply(lambda x: ipaddress.ip_network(x.strip(), strict=False))
    df['origin_as'] = df['as_path'].apply(lambda x: clean_as_path(x)[-1])  # Último AS es el origen

    trie = PatriciaTrie()
    for network in df['network'].drop_duplicates():
        trie.insert(network)

    # Consultar Whois en paralelo con el semáforo limitando las consultas
    unique_asns = df['origin_as'].unique()
    tasks = [is_asn_registered(asn) for asn in unique_asns]
    results = await asyncio.gather(*tasks)

    # Filtrar ASNs no registrados
    unregistered_asns = {}
    for asn, registered in zip(unique_asns, results):
        if not registered:
            unregistered_asns[asn] = df[df['origin_as'] == asn]['prefix'].tolist()

    # Mostrar resultados
   # print("\n🔍 **ASNs NO REGISTRADOS Y SUS PREFIJOS** 🔍")
  #  for asn, prefixes in unregistered_asns.items():
   #     print(f"ASN {asn} -> {len(prefixes)} prefijos asociados ❌")

    print(f"\n🔴 Total de ASNs no registrados: {len(unregistered_asns)}")
    print(f"🔴 Total de prefijos afectados por ASNs no registrados: {sum(len(prefixes) for prefixes in unregistered_asns.values())}")

    total_prefijos = len(df['network'].drop_duplicates())
    print(f"Total de prefijos únicos: {total_prefijos}")
    
    total_prefix_length = sum(network.prefixlen for network in df['network'].drop_duplicates())
    average_prefix_length = total_prefix_length / total_prefijos if total_prefijos else 0
    print(f"Average Prefix Length: {average_prefix_length:.2f}")

    # Cálculos de agregación
    max_agg_prefixes_count = 0
    aggregated_networks = set()

    # Agrupación por ASN
    for origin_as, group in df.groupby('origin_as'):
        networks = sorted(group['network'].drop_duplicates().tolist(), key=lambda x: (x.prefixlen, x.network_address))
        aggregated_in_as = set()

        # Barrido eficiente de redes por AS
        for network in networks:
            if network in aggregated_in_as:
                continue

            supernet_or_contiguous = trie.find_supernet_or_contiguous(network)
            if supernet_or_contiguous:
                max_agg_prefixes_count += 1
                aggregated_in_as.add(network)
                trie.mark_as_aggregated(network)
                aggregated_in_as.add(supernet_or_contiguous)

        aggregated_networks.update(aggregated_in_as)

    print(f"Maximum Aggregateable Prefixes: {max_agg_prefixes_count}")

    non_agg_prefixes_count = total_prefijos - max_agg_prefixes_count
    print(f"Unaggregateables Prefixes: {non_agg_prefixes_count}")

    factor_desagregacion = total_prefijos / len(aggregated_networks) if len(aggregated_networks) else 0
    print(f"Factor de desagregación: {factor_desagregacion:.2f}")

    # Longest and average AS Path
    df['as_path_length'] = df['as_path'].apply(lambda x: len(clean_as_path(x)))
    longest_as_path = df['as_path_length'].max()
    average_as_path_length = df['as_path_length'].mean()

    print(f"Longest AS-Path: {longest_as_path}")
    print(f"Average AS-Path: {average_as_path_length:.2f}")


# Ejecutar la función asíncrona
file_path = 'datos_columnas_filtradas.txt'
asyncio.run(analyze_ipv6_prefixes(file_path))
