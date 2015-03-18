# 在这里需要限定需要统计的RPC类型
# 需要统计的RPC类型包括：
# 将窗口内的统计数据通过kafka发送出去

import subprocess
import time
import json
import os
from kafka.client import KafkaClient
# from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
# from kafka.producer import KeyedProducer

class KafkaTopic:
    """define a kafka topic"""
    kafka_ip = ""
    kafka_port = ""
    topic_name = ""
    def __init__(self,kafka_cluster_ip,kafka_cluster_port,topic_name):
        self.kafka_ip = kafka_cluster_ip
        self.kafka_port = kafka_cluster_port
        self.topic_name = topic_name
    def sendMessage(self,message,batch_switch=False,batch_number=15,batch_time=15):
        kafka = KafkaClient(self.kafka_ip+':'+self.kafka_port)
        producer = SimpleProducer(kafka,batch_send=batch_switch,batch_send_every_n=batch_number,batch_send_every_t=batch_time)
        producer.send_messages(self.topic_name,message)

class RPCCollector:
    """docstring for RPCCollector"""
    kafkaT = None
    window = 30
    hostIP = "localhost"
    def __init__(self, kafkaTopic, window):
        self.kafkaT = kafkaTopic
        self.window = window
        ip = os.popen("/sbin/ifconfig | grep 'inet addr' | awk '{print $2}'").read()
        ip = ip[ip.find(':')+1:ip.find('\n')]
        self.hostIP = ip
    def getRPCCount(self):
        stime = time.time()
        counts = {}
        sumCount = 0
        proc = subprocess.Popen(["tshark","-i","eth0"],stdout=subprocess.PIPE)
        for line in iter(proc.stdout.readline,''):
            lparts = line.split(" ")
            if lparts[6] == "NFS":
                print lparts
                op = lparts[8]+lparts[9]
                sumCount += 1
                if op in counts:
                    counts[op] = counts[op] + 1
                else:
                    counts[op] = 1
                print op,counts[op],sumCount
            if time.time() - stime >= self.window:
                proc.terminate()
                return counts,sumCount
		#for line in iter(proc.stdout.readline,''):
        #    if line[0] == "N":
        #        opline = line.split("\n")[0].split(":")[1]
        #        ops = opline.split(" ")[1:]
        #        sumCount += len(ops)
        #        for o in ops:
        #            if o in counts:
        #                counts[o] = counts[o] + 1
        #            else:
        #                counts[o] = 1
        #    if time.time()-stime >= self.window:
        #        return counts,sumCount
    def genMessage(self,rpccount):
        '''
        消息的最终形式为一个json字符串，其内容如下：
        {
        'node':一个字符串 -- 内容为本机名称或者本机IP地址,
        'total':一个数字    -- 内容为在窗口内所有的RPC操作的数量,
        'counts':一个字典 -- 内容为在窗口内每一种RPC操作的数量
        }
        '''
        message = {}
        message['node'] = self.hostIP
        message['total'] = rpccount[1]
        message['counts'] = rpccount[0]
        return json.dumps(message)
    def collect(self):
        if self.window <= 0:
            return
        while True:
            rpccount = self.getRPCCount()
            msg = self.genMessage(rpccount)
            self.kafkaT.sendMessage(msg)

if __name__ == '__main__':
    kafkaTopic = KafkaTopic("127.0.0.1",
                                "9092",
                                "messagetunnel")
    collector = RPCCollector(kafkaTopic,30)
    collector.collect()


