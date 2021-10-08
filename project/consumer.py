from confluent_kafka import Consumer, DeserializingConsumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
import pandas as pd
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import datetime
import dateutil.parser
import numpy as np
import time


#plt.rcParams["backend"] = "TkAgg"

running = True

c = DeserializingConsumer({
    'bootstrap.servers' : 'localhost:9092',
    'group.id' : 'mygroup',
    'key.deserializer' : StringDeserializer(),
    'value.deserializer' : StringDeserializer()
})

topic = ["temperature"]
consumer = c

def basic_consume():
    try:
        consumer.subscribe(topic)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code == KafkaError._PARTITION_EOF:
                    print ("%s reached end at offset %d\n") %(msg.topic(), msg.offset())
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                yield msg
    finally:
        consumer.close()

def shutdown():
    running = False

def msg_process(msg):
    print(msg)



fig, ax = plt.subplots()
#fig1, ax1 = plt.subplots()
xdata1, ydata1 = [], []
xdata2, ydata2 = [], []
xdata3, ydata3 = [], []
xdata4, ydata4 = [], []

line1, = ax.plot([], [], '-r', lw=2)
line2, = ax.plot([],[], '-b', lw=2)
line3, = ax.plot([], [], '-g', lw=2)
line4, = ax.plot([], [], '-k', lw=2)

def animate(frame):
    key = frame.key()
    csv = frame.value().split(',')
    #print(key)
    #print(frame.value())
    time = datetime.datetime.fromisoformat(csv[1][:-1])
    temp = csv[0]
    
    if time == 'None' or temp == 'None':
        pass
    else:
        if key == "Buoy 3":
            xdata1.append(time)
            ydata1.append(float(temp))
            line1.set_data(xdata1, ydata1)
        elif key == "smb_fortune_bay":
            xdata2.append(time)
            ydata2.append(float(temp))
            line2.set_data(xdata2, ydata2)
        elif key.startswith("POOLC"):
            xdata3.append(time)
            ydata3.append(float(temp))
            line3.set_data(xdata3, ydata3)
        elif key == "smb_st_johns":
            xdata4.append(time)
            ydata4.append(float(temp))
            line4.set_data(xdata4, ydata4)
        else:
            pass
    return line1,line2,line3,line4

def init():
    ax1.set_xlim(0, 2*np.pi)
    ax1.set_ylim(-1, 1)
    return lines

def initBuoy():
    ax.set_xlim(pd.Timestamp('2020-01-01'), pd.Timestamp('2021-09-24'))
    ax.set_ylim(-10, 30)
    return line1,line2,line3,line4


def update(frame):
    xdata1.append(frame)
    ydata2.append(np.sin(frame))
    #lines.set_data(xdata1, ydata2)
    #return lines
    

if __name__ == '__main__':
    #ani = animation.FuncAnimation(fig1, update, frames=np.linspace(0, 2*np.pi, 128),init_func=init, blit=True)
    ani = animation.FuncAnimation(fig, animate, interval=0, frames=basic_consume(), init_func=initBuoy, blit=True)
    #basic_consume(c)
    plt.show()
    #time.sleep(30)
    #shutdown()
