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
        TI_I_LOTE,
        TI_I_DATEVALUE_APROX,
        ROW_NUMBER() OVER (PARTITION BY TI_I_LOTE ORDER BY TI_I_DATEVALUE_APROX) AS fila
    FROM ti_1
),
FechasIntervalos AS (
    SELECT 
        TI_I_LOTE,
        MIN(CASE WHEN fila = 1 THEN TI_I_DATEVALUE_APROX END) AS start1,
        MAX(CASE WHEN fila = 2 THEN TI_I_DATEVALUE_APROX END) AS end1,
        MIN(CASE WHEN fila = 3 THEN TI_I_DATEVALUE_APROX END) AS start2,
        MAX(CASE WHEN fila = 4 THEN TI_I_DATEVALUE_APROX END) AS end2
    FROM FechasOrdenadas
    GROUP BY TI_I_LOTE
)
SELECT DISTINCT
    ti_1.TI_I_LOTE, br_i.*
FROM br_i
JOIN FechasIntervalos f
  ON (br_i.BR_I_DATETIME BETWEEN f.start1 AND f.end1)
  OR (br_i.BR_I_DATETIME BETWEEN f.start2 AND f.end2)
JOIN ti_1 
  ON br_i.BR_I_ID_BIOREACTOR = ti_1.TI_I_ID_BIOREACTOR  
  AND ti_1.TI_I_LOTE = f.TI_I_LOTE;
"""

# Leer resultado de la consulta a un DataFrame
df = pd.read_sql(query, conn)

# Exportar a CSV
df.to_csv("BR_I_TI_1.csv", index=False)

# Cerrar la conexión
conn.close()
