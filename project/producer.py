from confluent_kafka import Producer
import socket
import requests
import json
import multiprocessing as mp

cmar_jordan_bay_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_fca0_698a_0716.jsonlKVP"
cmar_chedabucto_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_c5a5_c41c_2090.jsonlKVP"
cmar_stmarys_url = "https://cioosatlantic.ca/erddap/tabledap/cmar_8f10_9c65_13cb.jsonKVP"
fortunes_bay_url = "https://www.smartatlantic.ca/erddap/tabledap/DFO_Sutron_POOLC.jsonlKVP"
ios_moored = "https://data.cioospacific.ca/erddap/tabledap/IOS_ADCP_Moorings.jsonlKVP"
sa_stjohns_url = "https://www.smartatlantic.ca/erddap/tabledap/SMA_st_johns.jsonlKVP?time%3E=2020-01-01T00%3A00%3A00Z&time%3C=2021-09-24T02%3A30%3A01Z"
fortune_bay_buoy_url = "https://www.smartatlantic.ca/erddap/tabledap/SMA_Fortune_Bay_Buoy.jsonlKVP?time%3E=2020-07-11T00%3A00%3A00Z&time%3C=2021-09-24T01%3A27%3A00Z"

conf = {'bootstrap.servers' : "localhost:9092", 'client.id' : socket.gethostname()}

jb = requests.get(cmar_jordan_bay_url, stream=True)
cb = requests.get(cmar_chedabucto_url, stream=True)
sm = requests.get(cmar_stmarys_url, stream=True)
im = requests.get(ios_moored, stream=True)
fb = requests.get(fortunes_bay_url, stream=True)
sj = requests.get(sa_stjohns_url, stream=True)
fbbuoy = requests.get(fortune_bay_buoy_url)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))

def addBuoyStreamToKafka(stream, topic="temperature"):
    producer = Producer(conf)
    for chunk in stream.iter_lines(decode_unicode=True):
        if chunk: 
            item = json.loads(chunk)
            if 'buoy_name' in item.keys():
                print("BUOY 3")
                producer.produce(topic, key=str(item['buoy_name']), value=str(item['temp_water_c']) + ',' + str(item['time']))
                producer.poll(4)
            elif 'station_name' in item.keys():
                print("Not BUOY 3")
                if 'surface_temp_avg' in item.keys():
                    producer.produce(topic, key=str(item['station_name']), value=str(item['surface_temp_avg']) + ',' + str(item['time']))
                else: 
                    producer.produce(topic, key=str(item['station_name']), value=str(item['water_temperature']) + ',' + str(item['time']))
                producer.poll(4)
            else:
                pass
    

if __name__ == '__main__':
    p1 = mp.Process(target=addBuoyStreamToKafka, args=(cb,))
    p2 = mp.Process(target=addBuoyStreamToKafka, args=(fb,))
    p3 = mp.Process(target=addBuoyStreamToKafka, args=(fbbuoy,))
    p4 = mp.Process(target=addBuoyStreamToKafka, args=(sj,))
    p1.start()
    p2.start()
    p3.start()
    p4.start()