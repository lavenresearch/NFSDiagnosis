import json
import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer


class KafkaTopic:
    """define a kafka topic"""
    kafka_ip = ""
    kafka_port = ""
    topic_name = ""
    def __init__(self,kafka_cluster_ip,kafka_cluster_port,topic_name):
        self.kafka_ip = kafka_cluster_ip
        self.kafka_port = kafka_cluster_port
        self.topic_name = topic_name
    def receiveMessage(self):
        kafka = KafkaClient(self.kafka_ip+':'+self.kafka_port)
        consumer = SimpleConsumer(kafka, "operationAnalyser", self.topic_name)
        return consumer

class OperationAnalyser:
    kafkaT = None
    def __init__(self, kafkaTopic):
        self.kafkaT = kafkaTopic
    def receiveMsg(self):
        rawMsgs = self.kafkaT.receiveMessage()
        for rawMsg in rawMsgs:
            print rawMsg
            msg = json.loads(rawMsg.message.value)
            counts = msg['counts']
            sortedCounts = sorted(counts.iteritems(), key=lambda counts : counts[1], reverse=True)
            print "##############"+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"###############"
            print "node : " + msg['node']
            print "total : " + str(msg['total'])
            for i in xrange(len(sortedCounts)):
                print sortedCounts[i]


if __name__ == '__main__':
    kafkaTopic = KafkaTopic("192.168.3.130",
                            "9092",
                            "messagetunnel")
    analyser = OperationAnalyser(kafkaTopic)
    analyser.receiveMsg()
