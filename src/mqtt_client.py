import os
import paho.mqtt.client as mqtt
import json
from dotenv import load_dotenv
import requests
from db_operations import DBOperations
import sys
from logger import Logger

class MQTTClient:
    def __init__(self):
        load_dotenv(override=True)  # Cargar las variables de entorno desde el archivo .env
        self.IP_MQTT            = str(os.getenv('IP_MQTT')) 
        self.PORT               = int(os.getenv('PORT'))
        self.PASS_MQTT          = str(os.getenv('PASS_MQTT')) 
        self.USERNAME           = str(os.getenv('USERNAME'))
        self.URL                = str(os.getenv('URL'))

        self.client                 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect      = self.on_connect
        self.client.on_message      = self.on_message
        self.client.on_disconnect   = self.on_disconnect

        self.db_operations          = DBOperations()
        self.subscribed             = False  # Control de subscripción
        

    
    def connect(self):
        try:
            self.client.username_pw_set(self.USERNAME, self.PASS_MQTT)
            self.client.connect(self.IP_MQTT, self.PORT)
            self.client.loop_forever()
        except Exception as e:
            print(f"Error al conectar: {e}", file=sys.stderr)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            if not self.subscribed:
                responseMqtt = client.subscribe("v3/test-bob1@scs/devices/#")
                self.subscribed = True
                print(f"Subscripción MQTT exitosa: {responseMqtt}", file=sys.stdout)
        else:
            print(f"Error al conectar, código: {reason_code}", file=sys.stderr)     

    def on_message(self, client, userdata, msg):
     # Decodificar el mensaje recibido
        try:
            parsedResponse = json.loads(msg.payload.decode())

        except json.JSONDecodeError as e:
            print(f"Error al decodificar el JSON: {e}" , file=sys.stderr)
            return
        
        if parsedResponse:

            try:
                mac_sensor      = parsedResponse["end_device_ids"]["dev_eui"] 
                decoded_payload = parsedResponse["uplink_message"]["decoded_payload"]
                hex_value       = decoded_payload["hex"]
                rssi            = parsedResponse["uplink_message"]["rx_metadata"][0]["rssi"]
                

                response = requests.get(self.URL + hex_value)
    
                if response.status_code == 200:
                    response_json  = response.json()
                    self.db_operations.insert_data(response_json , rssi, mac_sensor , hex_value)
                else:
                    print(f"Error en la petición: {response.status_code}", file=sys.stdout)

            except KeyError as e:
                Logger.error("Error al procesar el JSON: {e}")

        else:
            print("Error al parsear el JSON" , file=sys.stderr)
    
    def on_disconnect(self, client, userdata, rc, *args, **kwargs):
        print(f"Desconexión detectada con código: {rc}", file=sys.stderr)
        if rc != 0:  # Desconexión inesperada
            print("Intentando reconectar...", file=sys.stderr)
            try:
                self.client.reconnect()
            except Exception as e:
                print(f"Error al intentar reconectar: {e}", file=sys.stderr)
