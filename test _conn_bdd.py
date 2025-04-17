from sqlalchemy import create_engine

conn_str = "mssql+pyodbc://@victus_camille/LAB_ATL_PIPELINE?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str)

try:
    connection = engine.connect()
    try:
        print("Connexion réussie à la base de données.")
    finally:
        connection.close()
except Exception as e:
    print(f"Erreur de connexion : {e}")