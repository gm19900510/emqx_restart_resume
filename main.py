# -*- coding: utf-8 -*-
from flask import Flask, request
import argparse
from messages_handle import MessagesHandle
from mqtt_handle import MqttHandle
# 使用多进程，进程队列
# from multiprocessing import Process, Queue
# messages_queue = Queue()

# 使用多线程，线程队列
import queue, threading
messages_queue = queue.Queue()
app = Flask(__name__)

 
@app.route('/webHook', methods=['GET', 'POST'])
def webHook():
    obj = request.get_json(force=True)
    messages_queue.put(obj)
    return "200"

   
def messages_handle_thread(redis_host,mqtt_host, mqtt_web_port, messages_queue):
    messagesHandle = MessagesHandle(redis_host,mqtt_host, mqtt_web_port) 
    messagesHandle.consumer_queue(messages_queue)


def mqtt_handle_thread(mqtt_host, mqtt_port):
    mqttHandle = MqttHandle(mqtt_host, mqtt_port) 
    mqttHandle.connect()

     
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='emqx 持久化恢复服务')
   
    parser.add_argument('-mqtt_host', '--mqtt_host', default='127.0.0.1', help='')
    parser.add_argument('-mqtt_port', '--mqtt_port', default=1883, help='')
    parser.add_argument('-mqtt_web_port', '--mqtt_web_port', default=18083, help='')
    parser.add_argument('-redis_host', '--redis_host', default='192.168.3.86', help='')
    parser.add_argument('-main_port', '--main_port', default=8881, help='')
    args = parser.parse_args()
    print('服务开启')
    # 开启进程
    # p1 = Process(target=handle, args=(args.redis_host, messages_queue))
    # p1.start()
    
    # 开启线程 
    t = threading.Thread(target=messages_handle_thread, args=(args.redis_host, args.mqtt_host, args.mqtt_web_port, messages_queue))
    t.start()
    
    t2 = threading.Thread(target=mqtt_handle_thread, args=(args.mqtt_host, args.mqtt_port))
    t2.start()

    app.run(host='0.0.0.0', port=args.main_port)
