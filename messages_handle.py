# -*- coding: utf-8 -*-
import redis
import requests
import json
import time
import traceback
host = '127.0.0.1'
port = 18083


# 创建订阅
def subscribe(client_id, topic, qos):     
    postdata = {
      "topic": topic,
      "qos": qos,
      "client_id": client_id
    }

    postdata = json.dumps(postdata) 
    print(postdata)
    r = requests.post('http://' + host + ':' + str(port) + '/api/v3/mqtt/subscribe', data=postdata, auth=('admin', 'public'))
    print(r.text)


# 创建批次订阅
def subscribe_batch(batch_topic):  
    postdata = json.dumps(batch_topic) 
    print(postdata)
    r = requests.post('http://' + host + ':' + str(port) + '/api/v3/mqtt/subscribe_batch', data=postdata, auth=('admin', 'public'))
    print(r.text)


def publish_batch(batch_payload):  
    postdata = json.dumps(batch_payload) 
    print(postdata)
    r = requests.post('http://' + host + ':' + str(port) + '/api/v3/mqtt/publish_batch', data=postdata, auth=('admin', 'public'))
    print(r.text)


# 断开指定连接
def disconnection(client_id):
    r = requests.delete('http://' + host + ':' + str(port) + '/api/v3/connections/' + client_id, auth=('admin', 'public'))
    print(r.text)


class MessagesHandle():
    
    def __init__(self, redis_host='127.0.0.1', mqtt_host='127.0.0.1', mqtt_web_port=18083): 
        flag = True
        while flag:
            try :
                self.pool = redis.ConnectionPool(host=redis_host)  # 实现一个连接池
                self.red = redis.Redis(connection_pool=self.pool)
                flag = False 
                global host, port
                host = mqtt_host
                port = mqtt_web_port
            except:
                traceback.print_exc()
                time.sleep(1)        
        
    def consumer_queue (self, messages_queue):
        while True:
            try :
                MQTT_RETAINER_NAME = 'mqtt_retainer'
                SPECIAL_CLIENT_ID = 'emqx_restart_retainer_plugin_by_gm' 
                
                message = messages_queue.get()  
                action = message['action']
                print('--->', message)
                if action == 'session_subscribed':
                    # 新增订阅
                    client_id = message['client_id']
                    topic = message['topic']
                    opts = message['opts']
                    qos = opts['qos']
                    self.red.sadd(client_id, topic + ';' + str(qos))
                   
                elif action == 'client_connected':
                    # 连接成功，调用web Api进行历史订阅
                    client_id = message['client_id']
                    s = self.red.smembers(client_id)
                    batch_topic = []
                    
                    # 指定用户上线，利用它的身份进行持久化消息转发
                    if client_id == SPECIAL_CLIENT_ID:
                        s = self.red.hgetall(MQTT_RETAINER_NAME)
                        batch_payload = []
                        for (k, v) in  s.items():
                            topic = str(k, encoding="utf-8")
                            v = str(v, encoding="utf-8")
                            qos = str(v.split(';;')[1])
                            payload = str(v.split(';;')[0])
                            postdata = {
                              "topic": topic,
                              "qos": int(qos),
                              "client_id": MQTT_RETAINER_NAME,
                              'payload':payload,
                              'retain':True
                            }
                            batch_payload.append(postdata)
                        if batch_payload:    
                            publish_batch(batch_payload)     
                        disconnection(client_id) 
                    else:    
                        for obj in s:
                            obj = str(obj, encoding="utf-8")  
                            topic = str(obj.split(';')[0])
                            qos = int(obj.split(';')[1])
                    
                            postdata = {
                              "topic": topic,
                              "qos": qos,
                              "client_id": client_id
                            }
                            batch_topic.append(postdata)
                        if batch_topic:    
                            subscribe_batch(batch_topic)
                    
                elif action == 'session_unsubscribed':
                    # 移除订阅
                    client_id = message['client_id']
                    topic = message['topic']
                    self.red.srem(client_id, topic + ';0')
                    self.red.srem(client_id, topic + ';1')
                    self.red.srem(client_id, topic + ';2')   
            
                elif action == 'message_publish':
                    if message['retain']:
                        topic = message['topic']
                        qos = message['qos']
                        payload = message['payload']
                      
                        self.red.hset(MQTT_RETAINER_NAME, topic, payload + ';;' + str(qos))
                        print('持久化最后一条消息完成') 
            except:
                traceback.print_exc()    
        
