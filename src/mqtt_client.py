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
            # parsedResponse = json.loads(msg.payload.decode())

            parsedResponse = {
  "end_device_ids": {
    "device_id": "bob-scsequipos1",
    "application_ids": {
      "application_id": "test-bob1"
    },
    "dev_eui": "70B3D531C0002F2E",
    "join_eui": "70B3D531C1120021",
    "dev_addr": "27FD61A4"
  },
  "correlation_ids": [
    "gs:uplink:01J49WEERN6N16105D3W1P4R0B"
  ],
  "received_at": "2024-08-02T15:48:29.935389122Z",
  "uplink_message": {
    "session_key_id": "AZEAsCO+2eO60T7CJSNOfg==",
    "f_port": 1,
    "f_cnt": 44,
    "frm_payload": "cgAAAAAlPggAAAAAAAAAAAB9////////////",
    "decoded_payload": {
      "hex": "7200000000253e080000000000000000007dffffffffffffffffff"
    },
    "rx_metadata": [
      {
        "gateway_ids": {
          "gateway_id": "eui-a84041ffff2298dc",
          "eui": "A84041FFFF2298DC"
        },
        "time": "2024-08-02T15:48:29.475633Z",
        "timestamp": 1814866150,
        "rssi": -111,
        "channel_rssi": -111,
        "snr": -3.8,
        "frequency_offset": "-2340",
        "location": {
          "latitude": -28.1372569159298,
          "longitude": -69.6508594594593,
          "altitude": 450,
          "source": "SOURCE_REGISTRY"
        },
        "uplink_token": "CiIKIAoUZXVpLWE4NDA0MWZmZmYyMjk4ZGMSCKhAQf//IpjcEObRsuEGGgwIzf+ztQYQhMygmQIg8ITP8+iqLQ==",
        "channel_index": 2,
        "received_at": "2024-08-02T15:48:29.589833732Z"
      }
    ],
    "settings": {
      "data_rate": {
        "lora": {
          "bandwidth": 125000,
          "spreading_factor": 9,
          "coding_rate": "4/5"
        }
      },
      "frequency": "904300000",
      "timestamp": 1814866150,
      "time": "2024-08-02T15:48:29.475633Z"
    },
    "received_at": "2024-08-02T15:48:29.724413084Z",
    "consumed_airtime": "0.287744s",
    "packet_error_rate": 0.2857143,
    "locations": {
      "user": {
        "latitude": -28.136871826777096,
        "longitude": -69.65072380651931,
        "source": "SOURCE_REGISTRY"
      }
    },
    "network_ids": {
      "net_id": "000013",
      "ns_id": "EC656E0000102DF3",
      "tenant_id": "scs",
      "cluster_id": "nam1",
      "cluster_address": "nam1.cloud.thethings.industries",
      "tenant_address": "scs.nam1.cloud.thethings.industries"
    }
  }
}

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
