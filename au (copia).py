import subprocess
import requests
import re
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuración del correo
SMTP_SERVER = "smtp.gmail.com"  # Servidor SMTP de Gmail
SMTP_PORT = 587  # Puerto para STARTTLS
SMTP_USER = "informacioon.semanal@gmail.com"  # Tu correo (cambiar)
SMTP_PASS = "qllz hwrw yxxc qmgp"  # Tu contraseña (cambiar)
DESTINATARIO = "robinhidalgo169@gmail.com"  # Destinatario del correo

# Ruta de archivos
SAVE_PATH = "rib_latest.bz2"
RIB_OUTPUT_FILE = "datos_rib.txt"
FILTERED_OUTPUT_FILE = "datos_columnas_filtradas.txt"
LOG_PATH = "ejecucion.log"

# URL base de Route Views Chile
BASE_URL = "https://routeviews.org/route-views.chile/bgpdata/2025.02/RIBS/"

def obtener_archivo_rib_mas_reciente():
    """Obtiene el archivo RIB más reciente desde la URL dada."""
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        log("❌ Error al acceder a la página de archivos RIB.")
        return None

    archivos_rib = re.findall(r'(rib\.\d{8}\.\d{4}\.bz2)', response.text)
    if not archivos_rib:
        log("❌ No se encontraron archivos RIB.")
        return None
    
    archivo_rib = sorted(archivos_rib, reverse=True)[0]
    
    return f"{BASE_URL}{archivo_rib}"

def descargar_archivo(url, save_path):
    """Descarga un archivo desde una URL dada y lo guarda en save_path."""
    log(f"⬇️ Descargando archivo desde {url} ...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        log("✅ Descarga completada.")
        return True
    else:
        log("❌ Error al descargar el archivo.")
        return False

def ejecutar_script(comando):
    """Ejecuta un script de Python y retorna su salida."""
    try:
        resultado = subprocess.run(comando, text=True, capture_output=True, check=True)
        log(f"✅ {comando[1]} ejecutado correctamente.")
        return resultado.stdout  # Devolvemos la salida del script
    except subprocess.CalledProcessError as e:
        log(f"❌ Error ejecutando {comando[1]}: {e}")
        return None

def enviar_correo(asunto, mensaje):
    """Envía un correo con las métricas obtenidas."""
    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = DESTINATARIO
        msg["Subject"] = asunto

        msg.attach(MIMEText(mensaje, "plain"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, DESTINATARIO, msg.as_string())
        server.quit()

        log("✅ Correo enviado correctamente.")
    except Exception as e:
        log(f"❌ Error al enviar el correo: {e}")

def limpiar_archivos():
    """Elimina los archivos temporales generados."""
    for archivo in [RIB_OUTPUT_FILE, FILTERED_OUTPUT_FILE]:
        if os.path.exists(archivo):
            os.remove(archivo)
            log(f"🗑️ Archivo {archivo} eliminado.")

def log(mensaje):
    """Registra un mensaje con la fecha y hora en el archivo de log."""
    with open(LOG_PATH, "a") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

# Obtener la URL del último archivo RIB
url_rib = obtener_archivo_rib_mas_reciente()
if url_rib and descargar_archivo(url_rib, SAVE_PATH):
    # Ejecutar bgpdump
    log("⚙️ Procesando con bgpdump...")
    result = subprocess.run(["bgpdump", "-m", SAVE_PATH, "-O", RIB_OUTPUT_FILE])

    if result.returncode == 0:  
        log(" bgpdump ejecutado correctamente.")
        
        # Ejecutar filtrar_columnas.py
        ejecutar_script(["python3", "filtrar_columnas.py"])
        
        # Ejecutar d.py y capturar salida
        salida_d = ejecutar_script(["python3", "d.py"])
        
        if salida_d:
            # Enviar correo con las métricas
            semana_actual = datetime.now().isocalendar()[1]  # Obtener la semana del año
            asunto = f"Métricas IPv6 - Semana {semana_actual}"
            mensaje = f"Adjunto la informacion de las  métricas IPv6 obtenidas de esta semana:\n\n{salida_d}"
            enviar_correo(asunto, mensaje)
        
        # Limpiar archivos temporales
        limpiar_archivos()
    else:
        log(" bgpdump falló.")
else:
    log(" No se pudo descargar el archivo RIB.")
