from db_connection import DBConnection
import json
import sys
import math
from logger import Logger
import pymysql




class DBOperations:
    def __init__(self):
        self.conn = DBConnection().connection

    def insert_data(self, data , rssi , mac_sensor , hex_value):


        if self.conn is None or not self.conn.open:
            Logger.warning("Conexión a la base de datos cerrada, intentando reconectar...")
            self.conn = DBConnection().connection

        if self.conn is None:
            Logger.error("No se pudo establecer la conexión a la base de datos.")
            return

        
        try:
            with self.conn.cursor() as cursor:

                TYPE_REPORT     = data['type']      
                FFT             = data['msg'].get('fft') 
                FFT_STR         = json.dumps(FFT)   
                decoded_payload = hex_value    


             


                print(f"decoded_payload: {decoded_payload}" , file=sys.stdout)

                # Validacion  si existe sensor

                cursor.execute("SELECT mac,id_sensor , nombre FROM sensores WHERE mac = %s", (mac_sensor))
                sensor = cursor.fetchone()

                if sensor is None:
                    Logger.error("No existe el sensor.")
                    return

            # Diccionario con las estructuras SQL y los parámetros correspondientes
                report_config = {
                "report": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id,
                            mac_sensor, 
                            nombre_sensor, 
                            tipo_reporte, 
                            anomalylevel, 
                            vibrationpercentage, 
                            goodvibration, 
                            nbalarmreport, 
                            temperature, 
                            reportid, 
                            vibrationlevel, 
                            peakfrequencyindex, 
                            batterypercentage, 
                            operatingtime, 
                            rssi,
                            reportlength, 
                            badvibrationpercentage1020,
                            badvibrationpercentage2040, 
                            badvibrationpercentage4060, 
                            badvibrationpercentage6080, 
                            badvibrationpercentage80100,
                            anomalylevelto20last24h,
                            anomalylevelto50last24h,
                            anomalylevelto80last24h,
                            anomalylevelto20last30d,
                            anomalylevelto50last30d,
                            anomalylevelto80last30d,
                            anomalylevelto20last6mo,
                            anomalylevelto50last6mo,
                            anomalylevelto80last6mo,
                            totaloperatingtimeknown,
                            decoded_payload,
                            totalunknown1020,
                            totalunknown2040,
                            totalunknown4060,
                            totalunknown6080,
                            totalunknown80100,
                            created_at
                        ) VALUES (%s,%s, %s, %s,%s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
                    """,
                    "params": (
                        sensor['id_sensor'],
                        sensor['mac'],
                        sensor['nombre'],
                        data['type'], 
                        data['msg'].get('anomalylevel'),
                        data['msg'].get('vibrationpercentage'), 
                        data['msg'].get('goodvibration'),
                        data['msg'].get('nbalarmreport'), 
                        data['msg'].get('temperature'), 
                        data['msg'].get('reportid'),
                        data['msg'].get('vibrationlevel'), 
                        data['msg'].get('peakfrequencyindex'),
                        data['msg'].get('batterypercentage'),
                        data['msg'].get('operatingtime'),
                        rssi,
                        data['msg'].get('reportlength'),
                        data['msg'].get('badvibrationpercentage1020'),
                        data['msg'].get('badvibrationpercentage2040'),
                        data['msg'].get('badvibrationpercentage4060'),
                        data['msg'].get('badvibrationpercentage6080'),
                        data['msg'].get('badvibrationpercentage80100'),

                        data['msg'].get('anomalylevelto20last24h'),
                        data['msg'].get('anomalylevelto50last24h'),
                        data['msg'].get('anomalylevelto80last24h'),
                        data['msg'].get('anomalylevelto20last30d'),
                        data['msg'].get('anomalylevelto50last30d'),
                        data['msg'].get('anomalylevelto80last30d'),
                        data['msg'].get('anomalylevelto20last6mo'),
                        data['msg'].get('anomalylevelto50last6mo'),
                        data['msg'].get('anomalylevelto80last6mo'),

                        data['msg'].get('totaloperatingtimeknown'),
                        decoded_payload,
                        data['msg'].get('totalunknown1020'),
                        data['msg'].get('totalunknown2040'),
                        data['msg'].get('totalunknown4060'),
                        data['msg'].get('totalunknown6080'),
                        data['msg'].get('totalunknown80100'),

                    )
                },

                "learning": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id,
                            mac_sensor,
                            decoded_payload,
                            nombre_sensor,
                            tipo_reporte, 
                            learningpercentage, 
                            temperature, 
                            vibrationlevel, 
                            peakfrequencyindex, 
                            learningfromscratch,
                            rssi,
                            fft, 
                            created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,NOW())
                    """,
                    "params": (
                        sensor['id_sensor'],
                        sensor['mac'],
                        decoded_payload,
                        sensor['nombre'], 
                        data['type'], 
                        data['msg'].get('learningpercentage'),
                        data['msg'].get('temperature'),
                        data['msg'].get('vibrationlevel'),
                        data['msg'].get('peakfrequencyindex'), 
                        data['msg'].get('learningfromscratch'),
                        rssi,
                        FFT_STR
                    )
                },
                "alarm": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, mac_sensor, decoded_payload, nombre_sensor, tipo_reporte, anomalylevel, temperature, 
                            vibrationlevel, rssi, fft, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        sensor['id_sensor'],         # %s
                        sensor['mac'],               # %s
                        decoded_payload,             # %s
                        sensor['nombre'],            # %s
                        data['type'],                # %s
                        data['msg'].get('anomalylevel'),   # %s
                        data['msg'].get('temperature'),    # %s
                        data['msg'].get('vibrationlevel'), # %s
                        rssi,                        # %s
                        FFT_STR                      # %s
                    )
                },
                "startstop": {
                    "sql": """
                        INSERT INTO reporte_sensor (
                            sensor_id, mac_sensor, decoded_payload, nombre_sensor, tipo_reporte, state, batterypercentage, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    "params": (
                        sensor['id_sensor'],
                        sensor['mac'],
                        decoded_payload,
                        sensor['nombre'], 
                        data['type'], 
                        data['msg'].get('state'),
                        data['msg'].get('batterypercentage')
                    )
                }
            }
            

            

            # Obtener configuración para el tipo de reporte actual
                config = report_config.get(TYPE_REPORT)
                
                if config:
                    cursor.execute(config["sql"], config["params"])

                    sql_update_sensor = """
                        UPDATE sensores
                        SET status_time_alert = %s,   -- nuevo valor de estado/alerta
                            updated_at = NOW()        -- timestamp de la actualización
                        WHERE mac = %s                 -- identificador único del sensor
                    """
                    params_update_sensor = (0, mac_sensor)
                    
                    # 3) Ejecutar UPDATE
                    cursor.execute(sql_update_sensor, params_update_sensor)
                    
                                    

                    self.conn.commit()
                    Logger.info(f"Tipo de reporte: {TYPE_REPORT} insertado correctamente")


                else:
                    Logger.warning(f"Tipo de reporte desconocido: {TYPE_REPORT}")

        except pymysql.OperationalError as e:
            if e.args[0] in [2006, 2013]:
                Logger.warning("Conexión perdida, intentando reconectar...")
                self.conn = DBConnection().connection
                self.insert_data(data, rssi, mac_sensor, hex_value)  # Reintentar
            else:
                Logger.error(f"Error al insertar datos: {e}")
        except Exception as e:
            Logger.error(f"Error al insertar datos: {e} , {TYPE_REPORT} , {data}")

        
