# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import time
import traceback
flag = True


# 用于响应服务器端 CONNACK 的 callback，如果连接正常建立，rc 值为 0
def on_connect(client, userdata, flags, rc):
    print("Connection returned with result code:" + str(rc))


# 用于响应服务器端 PUBLISH 消息的 callback，打印消息主题和内容
def on_message(client, userdata, msg):
    print("Received message, topic:" + msg.topic + "payload:" + str(msg.payload))


# 在连接断开时的 callback，打印 result code
def on_disconnect(client, userdata, rc):
    print("Disconnection returned result:" + str(rc))
    global flag
    flag = True
    client.loop_stop()
    client.disconnect()


def on_socket_close(client, userdata):
    print(client, userdata)

    
class MqttHandle():
    
    def __init__(self, mqtt_host, mqtt_port):
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

    def connect(self):    
        # 构造一个 Client 实例
        client = mqtt.Client(client_id='emqx_restart_retainer_plugin_by_gm', clean_session=False)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.on_socket_close = on_socket_close
        
        global flag
        while flag:
            try:    
                # 连接 broker
                # connect() 函数是阻塞的，在连接成功或失败后返回。如果想使用异步非阻塞方式，可以使用 connect_async() 函数。
                client.connect(self.mqtt_host, self.mqtt_port, 60)
                flag = False
  
            except:
                traceback.print_exc()
                time.sleep(1)     
        client.loop_start()
    
        while not flag:
            
            # 断开连接
            time.sleep(5)  # 等待消息处理结束
        print("退出MqttHandle线程")
