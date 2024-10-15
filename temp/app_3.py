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
        TI_CF_LOTE,
        TI_CF_DATEVALUE_APROX,
        ROW_NUMBER() OVER (PARTITION BY TI_CF_LOTE ORDER BY TI_CF_DATEVALUE_APROX) AS fila
    FROM ti_2
),
FechasIntervalos AS (
    SELECT 
        TI_CF_LOTE,
        MIN(CASE WHEN fila = 1 THEN TI_CF_DATEVALUE_APROX END) AS start1,
        MAX(CASE WHEN fila = 2 THEN TI_CF_DATEVALUE_APROX END) AS end1,
        MIN(CASE WHEN fila = 3 THEN TI_CF_DATEVALUE_APROX END) AS start2,
        MAX(CASE WHEN fila = 4 THEN TI_CF_DATEVALUE_APROX END) AS end2
    FROM FechasOrdenadas
    GROUP BY TI_CF_LOTE
)
SELECT DISTINCT
    ti_2.TI_CF_LOTE, br_cf.*
FROM br_cf
JOIN FechasIntervalos f
  ON (br_cf.BR_CF_DATETIME BETWEEN f.start1 AND f.end1)
  OR (br_cf.BR_CF_DATETIME BETWEEN f.start2 AND f.end2)
JOIN ti_2 
  ON br_cf.BR_CF_ID_BIOREACTOR = ti_2.TI_CF_ID_BIOREACTOR  
  AND ti_2.TI_CF_LOTE = f.TI_CF_LOTE;
"""

# Leer resultado de la consulta a un DataFrame
df = pd.read_sql(query, conn)

# Exportar a CSV
df.to_csv("BR_CF_TI_2.csv", index=False)

# Cerrar la conexión
conn.close()
