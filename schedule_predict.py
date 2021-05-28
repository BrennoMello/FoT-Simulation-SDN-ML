import time
import threading
from threading import Thread
import sys
sys.path.insert(0, '/home/openflow/predict')
import paho.mqtt.client as mqtt
from reg import utils_hosts 
import timeit
import os
import random


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
					os.system('python install_flow.py -g '+l_sensors[i].gateway_ip+' -s '+l_sensors[i].sensor_ip+' &')
					remove_flow(l_sensors[i].gateway_ip,l_sensors[i].sensor_ip)
					break
				#print(l_sensors[i].name_device, "Current time:", l_sensors[i].current_time,"Predict Time:",l_sensors[i].predict_time)
				l_sensors[i].current_time=timeit.default_timer()-l_sensors[i].init_time+0.0057
		except IndexError:
			continue
		except ValueError:
			print "ValueError Resolved"
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
				print "Installing "+l_sensors[i].name_device+" Publish "+str(l_sensors[i].publish_time)+" Name gateway "+l_sensors[i].name_gateway+" Current Time "+str(l_sensors[i].current_time)
	
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
				ob=Preditive_obj()
				ob.name_device=name_device
				ob.sensor_ip=utils_hosts.return_host_per_name(name_device).ip
				ob.publish_time=0.0
				ob.name_gateway=name
				ob.gateway_ip=utils_hosts.return_host_per_name(name).ip
				ob.init_time=timeit.default_timer()
				ob.predict_time=random.uniform(5, 25.0)
				l_sensors.append(ob)
				#print_list(name_device)
		elif(topic.find('dev/')==0 and message.find("FLOW INFO temperatureSensor")==0):
			name_device=topic.replace('dev/','')
			if(contains_name(name_device)==True):
				modifi_time_publish(message,name_device)
				
				
	def on_connect(client, userdata, flags, rc):
		client.subscribe('#')
		
	def on_message(client, userdata, msg):
		MensagemRecebida = str(msg.payload)
		#print "Topico "+msg.topic+" Mensagem "+MensagemRecebida
		catch_message(msg.topic,MensagemRecebida)
		#print_list()
		#l_sensors.append(1)
		##Continuar pegando o topico e verificando se o nome ja esta na lista
	
	##Settings to paho mqtt
	print "Init "+name	
	try:
		client =mqtt.Client(client_id='', clean_session=True, userdata=None, protocol=mqtt.MQTTv31)
		client.on_connect = on_connect
		client.on_message = on_message
		client.connect(gateway, PortaBroker, KeepAliveBroker)
		client.loop_forever()
	except KeyboardInterrupt:
		print "\nCtrl+C saindo..."
		sys.exit(0)
##END THREAD FUNCTIONS
		

				
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
	try:
		time.sleep(4)
	except KeyboardInterrupt:
		print "\nCtrl+C saindo..."
		sys.exit(0)

