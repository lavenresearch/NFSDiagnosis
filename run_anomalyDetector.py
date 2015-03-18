from __future__ import division
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
        consumer = SimpleConsumer(kafka, "suyiAnomalyD", self.topic_name)
        return consumer

class AnomalyDetector:
    # operations = ["OPEN","LAYOUTGET","GETDEVICEINFO","LATOUTCOMMIT"]
    operations = ['COMMITReply','COMMITCall,','SETATTRCall,','ACCESSCall,','ACCESSReply','WRITECall,','WRITEReply','GETATTRReply','SETATTRReply','GETATTRCall,']
    kafkaT = None
    clusterSize = 1
    matrixSize = 10
    matrixForNodes = {}
    rawMessagesForNodes = {}
    matrixForGlobal = []
    startTime = 0

    def __init__(self,kafkaTopic,clusterSize,matrixSize):
        self.kafkaT = kafkaTopic
        self.clusterSize = clusterSize
        self.matrixSize = matrixSize

    def standardizeRawMessage(self,rawMsg):
        msg = json.loads(rawMsg)
        standardizedMsg = {}
        t = msg['total']
        c = msg['counts']
        p = []
        semiStandardMsg = []
        for i in xrange(len(self.operations)):
            count = c.get(self.operations[i])
            if count == None:
                p.append(0)
                semiStandardMsg.append(0)
            else:
                p.append(count/t)
                semiStandardMsg.append(count)
        p.append(1-sum(p))
        semiStandardMsg.append(t-sum(semiStandardMsg))
        standardizedMsg['percentage'] = p
        standardizedMsg['node'] = msg['node']
        if not self.rawMessagesForNodes.has_key(msg['node']):
            self.rawMessagesForNodes[msg['node']] = []
        self.rawMessagesForNodes[msg['node']].append(semiStandardMsg)
        return standardizedMsg

    def updateMatrix(self,matrix,vector):
        matrix.append(vector)
        if len(matrix) > self.matrixSize:
            matrix.pop(0)

    def updateGlobalMatrix(self):
        globalSemiVector = []
        forTest = []
        # print self.rawMessagesForNodes
        for m in self.rawMessagesForNodes.values():
            m0 = m.pop(0)
            forTest.append(m0)
            if globalSemiVector == []:
                globalSemiVector = m0
            else:
                for i in xrange(len(self.operations)):
                    globalSemiVector[i] += m0[i]
        # print globalSemiVector
        if globalSemiVector == [0]:
            print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
            print forTest
            print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
        vectorsForGlobal = []
        t = sum(globalSemiVector)
        for i in xrange(len(globalSemiVector)):
            if t == 0:
                vectorsForGlobal.append(0)
            else:
                vectorsForGlobal.append(globalSemiVector[i]/t)
        # print vectorsForGlobal
        self.updateMatrix(self.matrixForGlobal,vectorsForGlobal)

    def receiveMsg(self):
        self.startTime = time.time()
        rawMsgs = self.kafkaT.receiveMessage()
        for rawMsg in rawMsgs:
            msg = self.standardizeRawMessage(rawMsg.message.value)
            if not self.matrixForNodes.has_key(msg['node']):
                self.matrixForNodes[msg['node']] = []
            self.updateMatrix(self.matrixForNodes[msg['node']],msg['percentage'])
            angle = self.analyse(self.matrixForNodes[msg['node']])
            self.diagnosis(msg['node'],angle)
            if len(self.rawMessagesForNodes) < self.clusterSize:
                continue
            flag = True
            for vs in self.rawMessagesForNodes.values():
                if vs == []:
                    flag = False
            if flag:
                self.updateGlobalMatrix()
                #for x in xrange(len(self.matrixForGlobal)):
                #    print self.matrixForGlobal[x]
                angle = self.analyse(self.matrixForGlobal)
                self.diagnosis("Global",angle)

    def analyse(self,matrix):
        #print "####################################"
        #for i in xrange(len(matrix)):
        #    print matrix[i]
        #print "####################################"
        #print "len(self.operations):" + str(len(self.operations))
        #print "len(matrix[0]:" + str(len(matrix[0]))
        size = len(matrix)
        if size < self.matrixSize:
            return None
        baseVector = []
        for i in xrange(len(matrix[0])):
            e = 0
            for j in xrange(size-1):
                e += matrix[j][i]
            baseVector.append(e/(size-1))
        lastVector = matrix[size-1]
        dotTime = 0
        baseVactorLength = 0
        lastVectorLength = 0
        for i in xrange(len(matrix[0])):
            dotTime += baseVector[i]*lastVector[i]
            baseVactorLength += baseVector[i]*baseVector[i]
            lastVectorLength += lastVector[i]*lastVector[i]
        baseVactorLength = baseVactorLength**0.5
        lastVectorLength = lastVectorLength**0.5
        if baseVactorLength == 0 or lastVectorLength == 0:
            angle = -1
            return angle
        angle = dotTime/(baseVactorLength*lastVectorLength)
        return angle

    def diagnosis(self,node,angle):
        currentTime = time.time() - self.startTime
        print currentTime,node,angle

    def updateFaultDB(self):
        pass

    def createFaultDB(self):
        pass

    def searchFaultDB(self):
        pass


if __name__ == '__main__':
    kafkaTopic = KafkaTopic("127.0.0.1",
                            "9092",
                            "messagetunnel")
    detector = AnomalyDetector(kafkaTopic,1,10)
    detector.receiveMsg()
