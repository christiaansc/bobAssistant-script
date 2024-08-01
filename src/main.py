from mqtt_client import MQTTClient
import time
import sys



if __name__ == "__main__":
    mqtt_client = MQTTClient()
    
    while True:
        try:
            mqtt_client.connect()
        except Exception as e:
            print(f"Error: {e}" , file=sys.stderr)
            time.sleep(10)  # Espera 10 segundos antes de intentar reconectar


