from confluent_kafka import Consumer, KafkaError
import time, json, dateutil.parser
from threading import Thread
import sys
import argparse
from datetime import datetime
sys.path.insert(0, '/home/mininet/projeto_ml/FoT-Stream_Simulation/FoTStreamServer/tsDeep')
#sys.path.insert(0, '/home/mininet/FoT-Simulation/FoTStreamServer/conceptdrift/algorithms/')
import series
#import cusum
#import page_hinkley
#import ewma
import gc
import json
#import pywt
import Wavelet
import threading
from memory_profiler import profile
import time
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np


############## Parse Arguments
parser = argparse.ArgumentParser(prog='FoT-StreamServer', usage='%(prog)s [options]', description='FoT-Stream Server')
parser.add_argument('-n','--name', type=str, help='Server Name',required=True)
parser.add_argument('-i','--ip', type=str, help='Server IP',required=True)
parser.add_argument('-p','--port', type=str, help='Server Port',required=True)

brokerMQTT = '10.0.0.1'
portBrokerMQTT = 1883
keepAliveBrokerMQTT = 60
client = mqtt.Client(client_id = '', clean_session=True, userdata=None, protocol = mqtt.MQTTv31)



args = parser.parse_args()
kafka_local = args.ip+':'+args.port
kafka_remote = '18.218.147.104'
kafka_remote_port = '9092'
group = 'server'

objWavelet = Wavelet.Wavelet()

class SensorDataConsumer(object):
	kafka_consumer = None
	topic = ""
	group = ""
	Sensorid = ""
	DeviceId = ""
	last_concept = time.time()
	last_delay = 0
	
	def __init__ (self):
		global kafka_local, group
        
		#self.kafka_consumer = Consumer(conf)
		#print(kafka_local)
		
		self.init = True
		self.gatewaySensoresData = {}
		self.modelsLSTM = {}
		self.dicDetectors = {}
		self.inicializationModel = {}
		self.verifyTrain = {}
		self.path_log = ""
        ##self.loopKafka()
		
		#self.create_file_log()
		

	#@profile	
	#def process_messages(self, value, change, timestamp):
		
		#{"CODE":"POST","METHOD":"FLOW","HEADER":{"NAME":"ufbaino16"},"BODY":{"conceptDrift": True, "temperatureSensor":["19.9884"],"FLOW":{"publish":1000,"collect":1000}}}
		
		#if jsonData["gatewayID"] in self.gatewaySensoresData:
		#	for value in jsonData["values"]:
		#		self.gatewaySensoresData[jsonData["gatewayID"]].append(value)
		#	print ("insert")
		#else:
		#	self.gatewaySensoresData[jsonData["gatewayID"]] = []

			#self.dicDetectors[jsonData["gatewayID"]] = cusum.CUSUM()
			#self.dicDetectors[jsonData["gatewayID"]] =	page_hinkley.PH() 
			#self.dicDetectors[jsonData["gatewayID"]] = ewma.EWMA()
			#self.modelsLSTM[jsonData["gatewayID"]] = series.modelLSTM(jsonData["gatewayID"])
			#self.modelsLSTM['gateway01'] = series.modelLSTM(jsonData["gatewayID"])
			#self.inicializationModel[jsonData["gatewayID"]] = False
			#self.verifyTrain[jsonData["gatewayID"]] == False
			#print ("new")	 
		
		#print(self.gatewaySensoresData[jsonData["gatewayID"]])
		#self.check_windows()
		#lock.release() 

	@profile
	def train_lstm(self, data, indexGateway):
		self.modelsLSTM[indexGateway].create_model(data)
	
	def create_file_log(self):
		self.path_log = "server-log-data-"+'Timestamp-{:%Y-%m-%d-%H-%M-%S}'.format(datetime.now())
		f = open(self.path_log, "w+")
		f.close()
	
	def save_file_log(self, json_log):
		print ("save log")
		file_log = open(self.path_log, "a")
		#file_log.write(json_log)
		file_log.write(json.dumps(json_log))
		file_log.write('\n')
		#json.dump(json_log, file_log)
		file_log.close()
	
			
	@profile
	def check_windows(self, value, change, timestamp):
		#global last_concept
		#for indexGateway in self.gatewaySensoresData:
			#print len(sensoresData[indexSensor])
		#	if len(self.gatewaySensoresData[indexGateway]) >= 50:
		self.modelsLSTM['gateway01'] = series.modelLSTM('gateway01')
		
		print('------------  train for gateway --------------')
								
		#print("Compĺete List")
		#print(self.gatewaySensoresData[indexGateway])
		
		print('type')
		window_data = value.replace('[','').replace(']','').split(',')
		window_data_np=np.array(window_data)
		window_data_np=window_data_np.astype(float)	
		window_data = window_data_np.tolist()
		print(window_data)
		df_data = pd.DataFrame(window_data)
		if(change == "True" or self.init == True):
			df_data['concept'] = 1
			self.last_delay = timestamp - self.last_concept 
			df_data['delay'] = self.last_delay
			last_concept = timestamp
			self.init = False
		else:
			df_data['concept'] = 0
			df_data['delay'] = self.last_delay
		
		print(df_data)
		#print("Try Traning neural network Gateway " + indexGateway)
		
		df_data.to_csv('dataset_moteid-01.csv', mode='a', header=False)
		
		print("Save dataframe")
		#self.modelsLSTM['gateway01'].create_model(df_data)
				
							
		#self.gatewaySensoresData[indexGateway] = []
				
	@profile
	def run(self):
		#consumer.subscribe(pattern='^awesome.*')
		
		self.kafka_consumer.subscribe(['dev'])
    
        #nome_arquivo = "Log-" + self.deviceId + "-" + self.Sensorid + "-" + str(datetime.datetime.now()) + ".txt"
        #arquivo = open(nome_arquivo, 'w+')  
		print('TimeOut '+str(sys.maxsize))
        # Now loop on the consumer to read messages
		running = True
		while running:
			
			
			#message = self.kafka_consumer.poll(timeout=sys.maxsize)
			message = self.kafka_consumer.poll()
            #arquivo = open(nome_arquivo, 'w+')
			if message is None:
				continue
            
			if message.error():
				print("Consumer error: {}".format(message.error()))
				continue

			print('Received message: {}'.format(message.value().decode('utf-8')))
            
			jsonData = json.loads(message.value().decode('utf-8'))
            
			self.process_messages(jsonData)
			    
            # agrupar por mote-id 
            # media vetorzao 
            # com haar e sem haar
            # se for incial rodar rede 
            # concept drift == True
            
				# entrada rede LSTM 
				# treinar rede
				# ficar testando previsao
			
			# concept drift == False
				
				# testando previsao
            
            # a hipotese com haar e com concept drift menos dados e com RMSE igual
            # vetor com janela aleatoria
            # 30 bacht 
            # media do rmse - trafego - 
            # decaimento da acuracia
            
            #parsedJson = json.loads(message.value().decode('utf-8'))
			
            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T03:41:11.559","valueSensor":["30.87","30.42","30.19","30.23","30.35","30.1","30.06","30.41","30.54","30.6"],"delayFog":116,"WindowSize":201}
            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T01:26:11.559"}
            	
            #latency = parsedJson['localDateTime']
            #parsed_data_latency = dateutil.parser.parse(latency)
            #print("Get Latency " + latency)
            #print("Get Latency Parsed " + str(abs(datetime.datetime.now()-parsed_data_latency).seconds))
            #parsedJson['LatencyWindow'] = str(abs(datetime.datetime.now()-parsed_data_latency).seconds)
            #print(parsedJson)
            #arquivo = open(nome_arquivo, 'a')
            #arquivo.write(json.dumps(parsedJson))
            #arquivo.write('\n')
            #arquivo.close()

	def create_file_log(self):
		global path_log
		f = open(path_log, "w+")
		f.close()

	def parser_msg_value(self, data):
		if(str(data).find('BODY') != -1):
			try:
					
				body = data['BODY']
				value = body['temperatureSensor']
				change = body['conceptDrift']
				
				print("change: ", change)
				return value, change
			except Exception as inst:
				print(inst)

	#{"CODE":"POST","METHOD":"FLOW","HEADER":{"NAME":"ufbaino16"},"BODY":{"conceptDrift": True, "temperatureSensor":["19.98", "19.55"],"FLOW":{"publish":1000,"collect":1000}}}
	# funcao chamada quando uma nova mensagem do topico eh gerada
	def on_message(self, client, userdata, msg):
		# decodificando o valor recebido
		#v = unpack(">H",msg.payload)[0]
		#print (msg.topic + "/" + str(v))
		#print ("New Message")
		try:
			#print (msg.payload)
			dataRaw = json.loads(msg.payload)
			value, change = self.parser_msg_value(dataRaw)
			ts = time.time()
			#nameSensor = parser_sensor_name(dataRaw)
			
			#self.process_messages(value, change)
			
			
			
			#insert_window(nameSensor, value)
			self.check_windows(value, change, ts)
			
			#print value
			#print nameSensor
		except Exception as inst:
			print_exc()
			print(inst)

	
	# funcao chamada quando a conexao for realizada, sendo
	# entao realizada a subscricao
	def on_connect(self, client, userdata, flags, rc):
		try:
			client.subscribe("dev/#")
		except Exception as inst:
			print(inst)

	def config_mqtt(self):
		global brokerMQTT, portBrokerMQTT, keepAliveBrokerMQTT, client, path_log, args
		#try:
				
		print("Init Server Model")
		#cria um cliente para supervisao
		path_log = args.name+"-serverModel-log-data-"+'Timestamp-{:%Y-%m-%d-%H-%M-%S}'.format(datetime.now())
		#self.create_file_log()
		client.on_connect = self.on_connect
		client.on_message = self.on_message
		client.connect(brokerMQTT, portBrokerMQTT, keepAliveBrokerMQTT)
			
		client.loop_forever()
			
		#except Exception as inst:
			
		#	print_exc()
		#	print(inst)
		#	print "\nCtrl+C leav..."
		#	sys.exit(0)

###########Main
sensorData = SensorDataConsumer()
sensorData.config_mqtt()		 
	
			 
#DevicesConnected = '[{"id":"sc02","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}]'

#print(DevicesConnected)
#jsonDevicesConnected = json.loads(DevicesConnected)
#print(jsonDevicesConnected)

#for devices in jsonDevicesConnected:
#    if(devices['id']=='sc01' or devices['id']=='sc02' ):
#        for sensors in devices['sensors']:
#            print("dev."+devices['id']+"."+sensors['id'])
#            ConsumerSensorTemperature = SensorKafkaConsumer(devices['id'], sensors['id'],"dev."+devices['id']+"."+sensors['id'], "Fog-IME", "18.218.147.104:9092")
#            ConsumerSensorTemperature.start()
            
#ConsumerSensor = SensorKafkaConsumer()
#ConsumerSensor.run()
