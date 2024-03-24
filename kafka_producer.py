import daemonize
import time 
import json
from kafka import KafkaProducer
import requests

kafka_bootstrap_servers = '10.0.2.10:9092'
kafka_topic_name = 'weathertopic'

def run_kafka():
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    api_key = 'YOUR_API_KEY'

    def get_weather_details(api_endpoint, params, city_name):
        api_response = requests.get(api_endpoint, params=params)
        json_data = api_response.json()
        temperature = json_data["data"]["values"]["temperature"]
        humidity = json_data["data"]["values"]["humidity"]
        wind_speed = json_data["data"]["values"]["windSpeed"]
        wind_direction = json_data["data"]["values"]["windDirection"]
        rain_intensity = json_data["data"]["values"]["rainIntensity"]
        json_message = {"CityName": city_name, "Temperature": temperature, "Humidity": humidity,
                        "WindSpeed": wind_speed, "WindDirection": wind_direction, "RainIntensity": rain_intensity,
                        "CreationTime": time.strftime("%Y-%m-%d %H:%M")}
        return json_message

    while True:
        tomorrow_api_endpoint = 'https://api.tomorrow.io/v4/weather/realtime'
        params = {"location": "tunis", "apikey": api_key}
        json_message = get_weather_details(tomorrow_api_endpoint, params, "Tunis")
        producer.send(kafka_topic_name, json_message)

        params = {"location": "sfax", "apikey": api_key}
        json_message = get_weather_details(tomorrow_api_endpoint, params, "Sfax")
        producer.send(kafka_topic_name, json_message)

        params = {"location": "mahdia", "apikey": api_key}
        json_message = get_weather_details(tomorrow_api_endpoint, params, "Mahdia")
        producer.send(kafka_topic_name, json_message)

        time.sleep(60)

if __name__ == "__main__":
    daemon = daemonize.Daemonize(app="weather_streaming", pid="/tmp/weather_streaming.pid", action=run_kafka)
    daemon.start()
