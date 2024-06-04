import os
import paho.mqtt.client as mqtt
import json
from dotenv import load_dotenv
import requests
from db_operations import DBOperations

class MQTTClient:
    def __init__(self):
        load_dotenv(override=True)  # Cargar las variables de entorno desde el archivo .env
        self.IP_MQTT            = str(os.getenv('IP_MQTT')) 
        self.PORT               = int(os.getenv('PORT'))
        self.PASS_MQTT          = str(os.getenv('PASS_MQTT')) 
        self.USERNAME           = str(os.getenv('USERNAME'))
        self.URL                = str(os.getenv('URL'))

        self.client             = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect  = self.on_connect
        self.client.on_message  = self.on_message

        self.db_operations = DBOperations()

    
    def connect(self):
        try:
            self.client.username_pw_set(self.USERNAME, self.PASS_MQTT)
            self.client.connect(self.IP_MQTT, self.PORT)
            self.client.loop_forever()
        except Exception as e:
            print(f"Error al conectar: {e}")

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            responseMqtt    = client.subscribe("/bobSCS")
            #parsedResponse = json.dumps(responseMqtt)
            parsedResponse  = "611C3B010044351C7F121D1D101D2F302B291E15110C090908080707060504030303020201020102"

            response        = requests.get(self.URL + parsedResponse)
            response_json   = json.dumps(response.json())
            resp_dic        = json.loads(response_json)

            self.db_operations.insert_data(resp_dic)

        else:
            print(f"Error al conectar, código: {reason_code}")

    def on_message(self, client, userdata, msg):
        print("Mensaje recibido en el tema " + msg.topic + ": " + str(msg.payload.decode()))