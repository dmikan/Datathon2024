import mysql.connector
import pandas as pd

# Conexión a la base de datos
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1030607277",
    database="aggity"
)

# Ejecutar consulta
query = """
SELECT * FROM ipc_cf;
"""

# Leer resultado de la consulta a un DataFrame
df = pd.read_sql(query, conn)

# Exportar a CSV
df.to_csv("IPC_CF.csv", index=False)

# Cerrar la conexión
conn.close()
