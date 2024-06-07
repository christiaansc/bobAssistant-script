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

                TYPE_REPORT = data['type']                

            # Diccionario con las estructuras SQL y los parámetros correspondientes
                report_config = {
                "report": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, nombre_sensor, tipo_reporte, anomalylevel, vibrationpercentage, 
                            goodvibration, nbalarmreport, temperature, reportid, vibrationlevel, 
                            peakfrequencyindex, batterypercentage, operatingtime, created_at
                        ) VALUES (11, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        data['sensor'], data['type'], data['msg'].get('anomalylevel'),
                        data['msg'].get('vibrationpercentage'), data['msg'].get('goodvibration'),
                        data['msg'].get('nbalarmreport'), data['msg'].get('temperature'), data['msg'].get('reportid'),
                        data['msg'].get('vibrationlevel'), data['msg'].get('peakfrequencyindex'),
                        data['msg'].get('batterypercentage'), data['msg'].get('operatingtime')
                    )
                },
                "alarm": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, nombre_sensor, tipo_reporte, anomalylevel, temperature, 
                            vibrationlevel, created_at
                        ) VALUES (11, %s, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        data['sensor'], data['type'], data['msg'].get('anomalylevel'),
                        data['msg'].get('temperature'), data['msg'].get('vibrationlevel')
                    )
                },
                "learning": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, nombre_sensor, tipo_reporte, learningpercentage, temperature, 
                            vibrationlevel, peakfrequencyindex, learningfromscratch, created_at
                        ) VALUES (11, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        data['sensor'], data['type'], data['msg'].get('learningpercentage'),
                        data['msg'].get('temperature'), data['msg'].get('vibrationlevel'),
                        data['msg'].get('peakfrequencyindex'), data['msg'].get('learningfromscratch')
                    )
                },
                "startstop": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, nombre_sensor, tipo_reporte, state, batterypercentage, created_at
                        ) VALUES (11, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        data['sensor'], data['type'], data['msg'].get('state'),
                        data['msg'].get('batterypercentage')
                    )
                }
            }

            # Obtener configuración para el tipo de reporte actual
                config = report_config.get(TYPE_REPORT)
                if config:
                    cursor.execute(config["sql"], config["params"])
                    self.conn.commit()
                    print(f"Tipo de reporte: {TYPE_REPORT} insertado correctamente")
                else:
                    print(f"Tipo de reporte desconocido: {TYPE_REPORT}")

        except Exception as e:
            print(f"Error al insertar datos: {e}")