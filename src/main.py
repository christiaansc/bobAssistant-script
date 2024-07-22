from mqtt_client import MQTTClient
import time



if __name__ == "__main__":
    mqtt_client = MQTTClient()
    
    while True:
        try:
            mqtt_client.connect()
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)  # Espera 10 segundos antes de intentar reconectar


