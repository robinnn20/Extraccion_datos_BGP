import asyncio
import pandas as pd
import ipaddress
import re
import json
import os
import time
from collections import defaultdict

# Variables
FILE_PATH = "datos_columnas_filtradas.txt"
SEMAPHORE_LIMIT = 50  # Reducido para evitar bloqueos en whois
semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
CACHE_FILE = "asn_cache.json"

# Cargar caché si existe
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        try:
            asn_cache = json.load(f)
        except json.JSONDecodeError:
            asn_cache = {}
else:
    asn_cache = {}

# Nodo del trie de Patricia
class PatriciaTrieNode:
    def __init__(self):
        self.children = {}
        self.network = None
        self.is_aggregated = False

# Trie de Patricia
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
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)
        supernet_candidate = None

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
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)[:network.prefixlen]
        for bit in prefix_str:
            node = node.children[bit]
        if node.network == network:
            node.is_aggregated = True

# Función para verificar si un ASN está registrado con reintentos
async def is_asn_registered(asn):
    """Verifica si un ASN está registrado usando WHOIS de RADB con reintentos."""
    if asn in asn_cache:
        return asn_cache[asn]

    async with semaphore:
        for attempt in range(1):  # Hasta 3 intentos
            try:
                process = await asyncio.create_subprocess_exec(
                    "whois", "-h", "whois.radb.net", f"AS{asn}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()

                try:
                    output = stdout.decode('utf-8')
                except UnicodeDecodeError:
                    output = stdout.decode('latin-1')

                # Expresión regular para detectar ASNs no registrados
                unregistered_pattern = re.compile(r"(denied|not match|not found|ERROR|no entries found|invalid)", re.IGNORECASE)
                registered = not bool(unregistered_pattern.search(output))

                asn_cache[asn] = registered
                save_cache()
                return registered

            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)  # Exponencial backoff (1s, 2s, 4s)
                else:
                    asn_cache[asn] = False
                    save_cache()
                    return False

# Guardar caché en archivo JSON
def save_cache():
    with open(CACHE_FILE, "w") as f:
        json.dump(asn_cache, f)

# Limpieza del AS Path
def clean_as_path(as_path):
    return as_path.replace("{", "").replace("}", "").replace(",", " ").split()

# Análisis de prefijos IPv6
async def analyze_ipv6_prefixes(file_path):
    df = pd.read_csv(file_path, delimiter='|', header=None, names=["prefix", "as_path"])
    df['network'] = df['prefix'].apply(lambda x: ipaddress.ip_network(x.strip(), strict=False))
    df['origin_as'] = df['as_path'].apply(lambda x: clean_as_path(x)[-1])

    trie = PatriciaTrie()
    for network in df['network'].drop_duplicates():
        trie.insert(network)

    total_prefijos = len(df['network'].drop_duplicates())
    total_prefix_length = sum(network.prefixlen for network in df['network'].drop_duplicates())
    average_prefix_length = total_prefix_length / total_prefijos if total_prefijos else 0

    print(f"Total prefixes: {total_prefijos}")
    print(f"Average Prefix Length: {average_prefix_length:.2f}")

    grouped_as = df.groupby('origin_as')
    max_agg_prefixes_count = 0
    aggregated_networks = set()

    for origin_as, group in grouped_as:
        networks = sorted(group['network'].drop_duplicates().tolist(), key=lambda x: (x.prefixlen, x.network_address))
        aggregated_in_as = set()

        for i in range(len(networks)):
            network = networks[i]
            if network in aggregated_in_as:
                continue

            supernet_or_contiguous = trie.find_supernet_or_contiguous(network)
            if supernet_or_contiguous:
                max_agg_prefixes_count += 1
                aggregated_in_as.add(network)
                trie.mark_as_aggregated(network)
                aggregated_in_as.add(supernet_or_contiguous)

        for network in aggregated_in_as:
            if not any(other_network.prefixlen > network.prefixlen and other_network.network_address == network.network_address for other_network in aggregated_in_as):
                aggregated_networks.add(network)

    print(f"Maximum Aggregateable Prefixes: {max_agg_prefixes_count}")
    print(f"Unaggregateables Prefixes: {total_prefijos - max_agg_prefixes_count}")
    print(f"Factor de desagregación: {total_prefijos / len(aggregated_networks) if aggregated_networks else 0:.2f}")

    df['as_path_length'] = df['as_path'].apply(lambda x: len(clean_as_path(x)))
    print(f"Longest AS-Path: {df['as_path_length'].max()}")
    print(f"Average AS-Path: {df['as_path_length'].mean():.2f}")

    print(f"Realizando consultas whois...")
    unique_asns = list(df['origin_as'].drop_duplicates())
    tasks = [is_asn_registered(asn) for asn in unique_asns]
    results = await asyncio.gather(*tasks)

    unregistered_asns = {}
    for asn, registered in zip(unique_asns, results):
        if not registered:
            unregistered_asns[asn] = df[df['origin_as'] == asn]['prefix'].tolist()

    print(f"Total unregistered ASNs: {len(unregistered_asns)}")
    print(f"Prefixes from unregistered ASNs in the Routing Table: {sum(len(prefixes) for prefixes in unregistered_asns.values())}")

# Ejecutar el análisis asíncrono
asyncio.run(analyze_ipv6_prefixes(FILE_PATH))
