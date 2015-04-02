import subprocess
import sys
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
        #proc = subprocess.Popen(["tshark","-i","ib0","-f","not arp"],stdout=subprocess.PIPE)
        while True:
            print "another while loop-----------------------------------------------------"
            proc = subprocess.Popen(["tshark","-i","ib0","-f","not arp"],stdout=subprocess.PIPE)
            for line in iter(proc.stdout.readline,''):
                nfsLoc = line.find("NFS")
                if nfsLoc > 0:
                    direction = line[nfsLoc:].split(" ")[3]
                    seqLoc = line.find("SEQUENCE")
                    if seqLoc < 0:
                       continue
                    ops = line[seqLoc:].split(" ")
                    for opindex in xrange(1,len(ops)):
                       op = ops[opindex]
                       op = direction+":"+op
                       sumCount += 1
                       if op in counts:
                           counts[op] = counts[op] + 1
                       else:
                           counts[op] = 1
            #for line in iter(proc.stdout.readline,''):
                #print line
                #lparts = line.split(" ")
                #if lparts[6] == "NFS":
                #    print lparts
                #    op = lparts[8]+lparts[9]
                #    sumCount += 1
                #    if op in counts:
                #        counts[op] = counts[op] + 1
                #    else:
                #        counts[op] = 1
                #    print op,counts[op],sumCount
            #while True:
            #    line = proc.stdout.read(1)
            #    sys.stdout.write(line)
            #    sys.stdout.flush()
            #    if line == '' and proc.poll() != None:
            #        break
            #    if line != '':
                    #sys.stdout.write("enter!!!")
                    #sys.stdout.flush()
            #        nfsLoc = line.find("NFS")
            #        if nfsLoc != -1:
            #            sys.stdout.write("\nfind NFS\n\n\n\n")
            #            sys.stdout.flush()
                        #sys.stdout.write(line)
                        #sys.stdout.flush()
            #            direction = line[nfsLoc:].split(" ")[3]
            #            seqLoc = line.find("SEQUENCE")
            #            if seqLoc == -1:
            #                continue
            #            ops = line[seqLoc:].split(" ")
            #            for opindex in xrange(1,len(ops)):
            #                op = ops[opindex]
            #                op = direction+":"+op
            #                sumCount += 1
            #                if op in counts:
            #                    counts[op] = counts[op] + 1
            #                else:
            #                    counts[op] = 1
                if time.time() - stime >= self.window:
                    # proc.terminate()
                    msg = self.genMessage(counts,sumCount)
                    print msg
                    self.kafkaT.sendMessage(msg)
                    stime = time.time()
                    counts = {}
                    sumCount = 0
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
    def genMessage(self,counts,sumCount):
        message = {}
        message['node'] = self.hostIP
        message['total'] = sumCount
        message['counts'] = counts
        return json.dumps(message)
    def collect(self):
        if self.window <= 0:
            return
        self.getRPCCount()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        ip = sys.argv[1]
        print "Detector IP: " + ip
    else:
        ip = "192.168.3.130"
    kafkaTopic = KafkaTopic(ip,"9092","messagetunnel")
    collector = RPCCollector(kafkaTopic,30)
    collector.collect()


