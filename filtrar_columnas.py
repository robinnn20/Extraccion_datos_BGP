import pandas as pd
import ipaddress
#Columnas
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
