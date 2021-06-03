import time
import threading
from threading import Thread
import sys
sys.path.insert(0, '/home/openflow/predict')
sys.path.insert(0, '/home/mininet/projeto_ml/FoT-Stream_Simulation/FoTStreamServer/tsDeep')
import paho.mqtt.client as mqtt
from reg import utils_hosts 
import timeit
import os
import random
import numpy as np
import json
import series
import pandas as pd


PortaBroker = 1883
KeepAliveBroker = 60
l_sensors=[]
break_all=False

class Preditive_obj(Thread):
	global publish_time, current_time, name_device, name_gateway, init_time, gateway_ip, sensor_ip, to_install
	def __init__ (self):
		self.publish_time=0
		self.predict_time=0.0
		self.current_time=0.0
		self.init_time=0.0
		self.name_device=''
		self.gateway_ip=''
		self.sensor_ip=''
		self.name_gateway=''
		self.to_install=True

def thread_flow(name,value):
	global l_sensors, break_all
	
	def is_flow(gateway_ip,sensor_ip,data):
		for i in range(0,len(data)):
			if(data[i].ip==sensor_ip and data[i].gateway==gateway_ip):
				return True
		return False
	
	def remove_flow(gateway_ip,sensor_ip):
		for i in range(0,len(l_sensors)):
			if(l_sensors[i].gateway_ip==gateway_ip and l_sensors[i].sensor_ip==sensor_ip):
				ob=l_sensors[i]
				l_sensors.remove(ob)
		
	
	
	while True:
		#print "Executing "+name
		time.sleep(0.2)
		#todo: criar copia de l_sensors e trabalhar dentro da repeticao com a copia
		#todo: usar l_sensors apenas para remover o fluxo e assim nao precisar usar o break toda vez
		try:
			#data=utils_hosts.return_association()
			for i in range(0,len(l_sensors)):
				if(float(l_sensors[i].current_time)>=(float(l_sensors[i].predict_time)*0.85)):	
					print("**** Installing Flow Device:",l_sensors[i].name_device, "Predict time:", l_sensors[i].predict_time,"Current time:",l_sensors[i].current_time)
					#os.system('python install_flow.py -g '+l_sensors[i].gateway_ip+' -s '+l_sensors[i].sensor_ip+' &')
					remove_flow(l_sensors[i].gateway_ip,l_sensors[i].sensor_ip)
					break
				#print(l_sensors[i].name_device, "Current time:", l_sensors[i].current_time,"Predict Time:",l_sensors[i].predict_time)
				l_sensors[i].current_time=timeit.default_timer()-l_sensors[i].init_time+0.0057
		except IndexError:
			continue
		except ValueError:
			print ("ValueError Resolved")
		#	continue
		except ValueError:
			continue

#THREAD
def thread(name,gateway):
	global l_sensors
	##INIT THREAD FUNCTIONS
	def contains_name(name_device):
		for i in range(0,len(l_sensors)):
			if(l_sensors[i].name_device==name_device and l_sensors[i].name_gateway==name):
				return True
		return False
	
	def print_list(name):
		for i in range(0,len(l_sensors)):
			if(l_sensors[i].name_device==name):
				print ("Installing "+l_sensors[i].name_device+" Publish "+str(l_sensors[i].publish_time)+" Name gateway "+l_sensors[i].name_gateway+" Current Time "+str(l_sensors[i].current_time))
	
	def message_to_publish(msg):
		
		#old way work just with init message flow
		#msg=msg.replace('FLOW INFO temperatureSensor ','')
		#msg=msg.replace("collect","\"collect\"")
		#msg=msg.replace("publish","\"publish\"")
		
		#new way work with any message flow
		msg=msg.rsplit('\"time\": ')[1].rsplit('}')[0]+'}'
		obj=utils_hosts.to_object(msg)
		return obj.publish
	
	def modifi_time_publish(msg,name_device):
		for i in range(0,len(l_sensors)):
			if(l_sensors[i].name_device==name_device):
				msg=msg.rsplit('FLOW INFO temperatureSensor ')[1]
				msg=msg.replace("collect","\"collect\"")
				msg=msg.replace("publish","\"publish\"")
				obj=utils_hosts.to_object(msg)
				ob=l_sensors[i]
				l_sensors.remove(ob)
				#l_sensors[i].publish_time=obj.publish
				break
				
	
	def catch_message(topic,message):
		#message correct TATU
		#print("MSG TATU", message)
		#"method": "flow"
		#print("MSG",message.find("\"method\": \"flow\""))
		#print("TOPIC",topic.find('/dev/'))
		if(topic.find('dev/')!=-1 and message.find("\"METHOD\":\"FLOW\"")!=-1):
			name_device=topic.replace('dev/','')
			#print("MSG TATU 2 "+str(name_device))
			#print("TATU message received from", name_device)
			#print_list(name_device)
			#print(name_device)			
			if(contains_name(name_device)==False):
				#print("Update from ",name_device)
				#pub=message_to_publish(message)
				#print("Breno eh mengao")
				#print(pub)
				
				#Obter os dados recebidos da mensagem TATU considerando que a mensagem esta na variavel 'a'
				#message=message.encode('utf-8')
				#index=0
				#message=message[0 : index : ] + message[index + 1 : :]
				print(message)
				obj=utils_hosts.to_object(message)
				#msg_json=json.loads(message)
				#msg_json=json.loads('{"CODE":"POST","METHOD":"FLOW","HEADER":{"NAME":"ufbaino01"},"BODY":{"conceptDrift":"False","temperatureSensor":"[21.0664, 21.086, 21.0566, 21.0664, 21.0664, 21.0566, 21.1252, 21.1644, 21.184, 21.2624, 21.2722, 21.2722, 21.2918, 21.282, 21.3212, 21.331, 21.3408, 21.3212, 21.331, 21.3408, 21.3604, 21.3604, 21.3408, 21.3604, 21.38, 21.3996, 21.3996, 21.38, 21.38, 21.38, 21.331, 21.3114, 21.3016, 21.3212, 21.3408, 21.3408, 21.282, 21.2624, 21.2624, 21.2428, 21.2036, 21.2232, 21.233, 21.2232, 21.2036, 21.184, 21.1252, 21.1252, 21.1056, 21.0272]","FLOW":{"publish":4000,"collect":4000}}}')
				#print(msg_json['HEADER'])
				#Nome do sensor
				print("Nome: "+obj.HEADER['NAME'])
				
				#Valor do conecpt Drift como Boolean e nao como string
				#print(eval(obj.BODY['conceptDrift']))
				
				#retorna a janela de dados em formato vetor e nao como string
				window_data=obj.BODY['temperatureSensor'].replace('[','').replace(']','').split(',')
				#print(window_data[0])
				window_data_np=np.array(window_data)
				window_data_np=window_data_np.astype(float)	
				window_data = window_data_np.tolist()
				#print(window_data)
				#print(getPredict(window_data))
				predict=getPredict(window_data)
				print(predict)
				predict=np.mean(predict)
				#print("Media")
				print("Tempo predito para",name_device,"=",predict)
				ob=Preditive_obj()
				ob.name_device=name_device
				ob.sensor_ip=utils_hosts.return_host_per_name(name_device).ip
				ob.publish_time=0.0
				ob.name_gateway=name
				ob.gateway_ip=utils_hosts.return_host_per_name(name).ip
				ob.init_time=timeit.default_timer()
				#ob.predict_time=random.uniform(5, 25.0)
				ob.predict_time=predict
				l_sensors.append(ob)
				#print_list(name_device)
		elif(topic.find('dev/')==0 and message.find("FLOW INFO temperatureSensor")==0):
			name_device=topic.replace('dev/','')
			if(contains_name(name_device)==True):
				modifi_time_publish(message,name_device)
				
				
	def on_connect(client, userdata, flags, rc):
		client.subscribe('#')
		
	def on_message(client, userdata, msg):
		MensagemRecebida = msg.payload.decode('utf-8')
		#print "Topico "+msg.topic+" Mensagem "+MensagemRecebida
		catch_message(msg.topic,MensagemRecebida)
		#print_list()
		#l_sensors.append(1)
		##Continuar pegando o topico e verificando se o nome ja esta na lista
	
	##Settings to paho mqtt
	print ("Init "+name)	
	#try:
	client =mqtt.Client(client_id='', clean_session=True, userdata=None, protocol=mqtt.MQTTv31)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(gateway, PortaBroker, KeepAliveBroker)
	client.loop_forever()
	#except KeyboardInterrupt:
	#	print "\nCtrl+C saindo..."
	#	sys.exit(0)
	#except Exception as e:
	#	print(e)
	#	sys.exit(0)
##END THREAD FUNCTIONS
		

def getPredict(window_data):
		print("getPredict")
		modelsLSTM={}
		modelsLSTM['gateway01'] = series.modelLSTM('gateway01')
		
		print('------------  train for gateway --------------')
								
		#print("Complete List")
		#print(self.gatewaySensoresData[indexGateway])
		
		df_data = pd.DataFrame(window_data)
		df_data['concept'] = 1
			
		
		
		print(df_data)
		#print("Try Traning neural network Gateway " + indexGateway)
		
		#modelsLSTM['gateway01'].create_model(df_data)
		return modelsLSTM['gateway01'].calc_rmse(df_data)
				
##start thread mqtt to each gateway
gateways=utils_hosts.return_hosts_per_type('gateway')
for i in range(0,len(gateways)):
	a = Thread(target=thread,args=(gateways[i].name_iot,gateways[i].ip))
	a.daemon=True
	a.start()
##End start thread to each gateway

for i in range(0,len(gateways)):
	a = Thread(target=thread_flow,args=(gateways[i].name_iot,gateways[i].ip))
	a.daemon=True
	a.start()
##End start thread to each gateway

#start thread mqtt_install_flow to each gateway


#LOOP To keep the prompt
while True:
	#try:
	time.sleep(4)
	#except KeyboardInterrupt:
	#	print "\nCtrl+C saindo..."
	#	sys.exit(0)
	#except Exception as e:
	#	print(e)
	#	sys.exit(0)
