from mqtt_client import MQTTClient
import time


if __name__ == "__main__":
    mqtt_client = MQTTClient()
    mqtt_client.connect()

    try:
        while True:
            time.sleep(1)  # Mantener el script corriendo
    except KeyboardInterrupt:
        print("Desconectando cliente MQTT...")
        mqtt_client.client.loop_stop()
        mqtt_client.client.disconnect()

