from db_connection import DBConnection

class DBOperations:
    def __init__(self):
        self.conn = DBConnection().connection

    def insert_data(self, data):
        if self.conn is None:
            print("No se pudo establecer la conexión a la base de datos.")
            return

        try:
            with self.conn.cursor() as cursor:
                sql = "INSERT INTO reporte_sensor (nombre, created_at) VALUES (%s, NOW())"
                cursor.execute(sql, (data['sensor']))
                self.conn.commit()
                print("Datos insertados correctamente")
        except Exception as e:
            print(f"Error al insertar datos: {e}")