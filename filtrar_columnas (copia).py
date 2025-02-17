import pandas as pd
import ipaddress

# Verifica si una dirección es IPv6
def is_ipv6(address):
    try:
        return ipaddress.ip_network(address.strip(), strict=False).version == 6
    except ValueError:
        return False

# Lee el archivo de entrada en un DataFrame, asumiendo que las columnas están separadas por '|'
df = pd.read_csv('datos_rib.txt', delimiter='|', header=None)

# Filtra solo las columnas 5 y 6 (recuerda que los índices de pandas empiezan en 0)
filtered_df = df.iloc[:, [5, 6]]

# Filtra direcciones IPv6
filtered_df = filtered_df[filtered_df[5].apply(is_ipv6)]

# Elimina duplicados basados en la columna 5
#unique_filtered_df = filtered_df.drop_duplicates(subset=[5])

# Guarda el resultado en un archivo de salida
filtered_df.to_csv('datos_columnas_filtradas.txt', sep='|', index=False, header=False)

print("El archivo de salida ha sido creado con los prefijos IPv6 únicos.")
