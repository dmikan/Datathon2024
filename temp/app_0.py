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
WITH FechasOrdenadas AS (
    SELECT 
        HC_LOTE,
        HC_DATEVALUE_APROX,
        ROW_NUMBER() OVER (PARTITION BY HC_LOTE ORDER BY HC_DATEVALUE_APROX) AS fila
    FROM hc
),
FechasIntervalos AS (
    SELECT 
        HC_LOTE,
        MIN(CASE WHEN fila = 1 THEN HC_DATEVALUE_APROX END) AS start1,
        MAX(CASE WHEN fila = 2 THEN HC_DATEVALUE_APROX END) AS end1,
        MIN(CASE WHEN fila = 3 THEN HC_DATEVALUE_APROX END) AS start2,
        MAX(CASE WHEN fila = 4 THEN HC_DATEVALUE_APROX END) AS end2
    FROM FechasOrdenadas
    GROUP BY HC_LOTE
)
SELECT DISTINCT
    hc.HC_LOTE, 
    c.*, 
    CASE 
        WHEN c.C_DATETIME BETWEEN f.start1 AND f.end1 THEN 1
        WHEN c.C_DATETIME BETWEEN f.start2 AND f.end2 THEN 2
        ELSE NULL
    END AS C_CENTRIFUGACION
FROM c
JOIN FechasIntervalos f
  ON (c.C_DATETIME BETWEEN f.start1 AND f.end1)
  OR (c.C_DATETIME BETWEEN f.start2 AND f.end2)
JOIN hc 
  ON c.C_ID_CENTRIFUGA = hc.HC_ID_CENTRIFUGA  
  AND hc.HC_LOTE = f.HC_LOTE;
"""

# Leer resultado de la consulta a un DataFrame
df = pd.read_sql(query, conn)

# Exportar a CSV
df.to_csv("_C_CH.csv", index=False)

# Cerrar la conexión
conn.close()
