from confluent_kafka import Producer
import socket
import requests
import json

cmar_jordan_bay_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_fca0_698a_0716.jsonlKVP"
cmar_chedabucto_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_c5a5_c41c_2090.jsonlKVP"
cmar_stmarys_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_8f10_9c65_13cb.jsonKVP"

conf = {'bootstrap.servers' : "localhost:9092", 'client.id' : socket.gethostname()}

jb = requests.get(cmar_jordan_bay_url, stream=True)
cb = requests.get(cmar_chedabucto_url, stream=True)
sm = requests.get(cmar_stmarys_url, stream=True)

producer = Producer(conf)

for chunk in cb.iter_lines(decode_unicode=True):
    if chunk: 
        item = json.loads(chunk)
        producer.produce("temperature", key=item['buoy_name'], value=str(item['temp_water_c']))

producer.poll(4)