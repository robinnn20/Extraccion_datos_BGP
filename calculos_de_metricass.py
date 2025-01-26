import pandas as pd
import ipaddress

class PatriciaTrieNode:
    def __init__(self):
        self.children = {}
        self.network = None

class PatriciaTrie:
    def __init__(self):
        self.root = PatriciaTrieNode()

    def insert(self, network):
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)
        
        for bit in prefix_str[:network.prefixlen]:
            if bit not in node.children:
                node.children[bit] = PatriciaTrieNode()
            node = node.children[bit]
        
        node.network = network

    def search(self, network):
        node = self.root
        prefix_str = bin(int(network.network_address))[2:].zfill(128)
        
        for bit in prefix_str[:network.prefixlen]:
            if bit in node.children:
                node = node.children[bit]
            else:
                return False
        
        return node.network == network

    def aggregate(self):
        combinable = []
        def _aggregate(node, current_prefix=""):
            if node.network:
                combinable.append(node.network)
            for bit in node.children:
                _aggregate(node.children[bit], current_prefix + bit)
        _aggregate(self.root)
        return combinable

def can_aggregate(network1, network2):
    try:
        if network1.prefixlen != network2.prefixlen:
            return False
        supernet = network1.supernet(new_prefix=network1.prefixlen - 1)
        return supernet.overlaps(network2)
    except ValueError:
        return False

def analyze_ipv6_prefixes(file_path):
    df = pd.read_csv(file_path, delimiter='|', header=None, names=["prefix", "as_path"])
    df['network'] = df['prefix'].apply(lambda x: ipaddress.ip_network(x.strip(), strict=False))
    df['origin_as'] = df['as_path'].apply(lambda x: x.split()[-1])

    trie = PatriciaTrie()
    for network in df['network'].drop_duplicates():
        trie.insert(network)

    total_prefijos = len(df['network'].drop_duplicates())
    print(f"Total de prefijos únicos: {total_prefijos}")

    total_prefix_length = sum(network.prefixlen for network in df['network'].drop_duplicates())
    average_prefix_length = total_prefix_length / total_prefijos if total_prefijos else 0
    print(f"Average Prefix Length: {average_prefix_length:.2f}")

    grouped_as = df.groupby('origin_as')
    max_agg_prefixes_count = 0

    for origin_as, group in grouped_as:
        networks = sorted(group['network'].drop_duplicates().tolist(), key=lambda x: (x.prefixlen, x.network_address))
        for network in networks:
            trie.insert(network)

    combinable_networks = trie.aggregate()
    for i in range(len(combinable_networks) - 1):
        if can_aggregate(combinable_networks[i], combinable_networks[i + 1]):
            max_agg_prefixes_count += 2

    print(f"Maximum Aggregateable Prefixes: {max_agg_prefixes_count}")

    non_agg_prefixes_count = total_prefijos - max_agg_prefixes_count
    print(f"Unaggregateables Prefixes: {non_agg_prefixes_count}")

    df['as_path_length'] = df['as_path'].apply(lambda x: len(x.split()))
    longest_as_path = df['as_path_length'].max()
    average_as_path_length = df['as_path_length'].mean()

    print(f"Longest AS-Path: {longest_as_path}")
    print(f"Average AS-Path: {average_as_path_length:.2f}")

file_path = 'datos_columnas_filtradas.txt'
analyze_ipv6_prefixes(file_path)
