import paho.mqtt.client as mqtt
import sys
import json
import time
import timeit
import random
from threading import Thread
import argparse
import fileinput
import data_set
import logging
from traceback import print_exc
from skmultiflow.drift_detection import PageHinkley
from skmultiflow.drift_detection.adwin import ADWIN



############## Parse Arguments
parser = argparse.ArgumentParser(prog='virtual_device', usage='%(prog)s [options]', description='Virtual Device')
parser.add_argument('-n','--name', type=str, help='Device Name',required=True)
parser.add_argument('-s','--sensor', type=str, help='Sensor Name',required=True)
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)
parser.add_argument('-i','--ip', type=str, help='Gateway IP',required=True)
parser.add_argument('-d','--direc', type=str, help='Direc Data Set',required=True)
parser.add_argument('-m','--mote', type=str, help='Mote id Sensor',required=True)
args = parser.parse_args()

#################### GLOBAL VARS
broker=args.ip
portBroker = args.port
keepAliveBroker = 60
topicSubscribe = "dev/"+args.name
client =mqtt.Client(client_id='', clean_session=True, userdata=None, protocol=mqtt.MQTTv31)
tatu_message_type=""
publish_msg=0
collect_msg=0
thread_use=False
sensorName=args.sensor
direc=args.direc
dataSet = None
moteid = args.mote
logging.basicConfig(filename = 'app.log', level = logging.INFO)
window_sensor = list()
ph = PageHinkley(min_instances=30, delta=0.005, threshold=5, alpha=0.9999)
adwin = ADWIN()


#json to Object
class to_object(object):
	print("json")
	def __init__(self, j):
		self.__dict__ = json.loads(j)

################# THREAD Flow Publish 
class Th(Thread):
	global publish_msg, sample, args, delta, data_samples, indice, samples_l, sensorName, direc, moteid
	
	def __init__ (self):
		print("Init Thread")
			
		try:
			self.dataSetReader = data_set.DataSetReader(direc, sensorName, moteid)	
			print("data set open:  "+ str(self.dataSetReader.next_value(sensorName)))
		except Exception as inst:
			print_exc()
			print(inst)
			logging.exception(str(inst))
		Thread.__init__(self)
		
	
	def run(self):
		global tatu_message_type, thread_use
		while thread_use==True:
			if(tatu_message_type=="flow"):
				try:
					ini=timeit.default_timer()
					a=self.publish()
					fim=timeit.default_timer()
					print (self.get_time_publish())
					print (float(fim-ini))
					#print float(self.get_time_publish())-float(fim-ini)
					time.sleep(float(self.get_time_publish())-float(fim-ini))
					
				except Exception as inst:
					print_exc()
					print(inst)
					logging.exception(str(inst))
			elif(tatu_message_type=="evt" and delta!=0):
				pass
	
	def get_value(self):
		return self.dataSetReader.next_value(sensorName)
		
		#return str(int(random.randint(18,37)))
		
	def window_check(self, data):
		global window_sensor, ph, adwin
		change = False
		window_sensor.append(data)
		
		if len(window_sensor) >= 35:
			for data in window_sensor:
				ph.add_element(data)
				print("Tentando Detectar")
				if ph.detected_change():
					print('Change has been detected in data: ')
					change = True
			#window_sensor = []
		
		return change
		
	def get_time_publish(self):
		global publish_msg
		return str(publish_msg/1000)

	def publish(self):
		global tatu_message_type, args, collect_msg, publish_msg, window_sensor
		
		new_value = self.get_value()
		print("Valor lido")
		print(new_value)
		change = self.window_check(new_value)
		print(change)
		#if(tatu_message_type=="flow" and len(window_sensor) >= 35 and change == True):
		if(tatu_message_type=="flow" and len(window_sensor) >= 35):
			#colocar o change no body
			# {"CODE":"POST","METHOD":"FLOW","HEADER":{"NAME":"ufbaino16"},"BODY":{"conceptDrift": +change+, "temperatureSensor":["19.98", "19.55"],"FLOW":{"publish":1000,"collect":1000}}}
			#a= "{\"CODE\":\"POST\",\"METHOD\":\"FLOW\",\"HEADER\":{\"NAME\":\""+str(args.name)+"\"},\"BODY\":{\""+str(args.sensor)+"\":[\""+str(window_sensor)+"\"],\"FLOW\":{\"publish\":"+str(publish_msg)+",\"collect\":"+str(collect_msg)+"}}}"
			#Nova string Ernando
			a= "{\"CODE\":\"POST\",\"METHOD\":\"FLOW\",\"HEADER\":{\"NAME\":\""+str(args.name)+"\"},\"BODY\":{\"conceptDrift\":\""+str(change)+"\",\""+str(args.sensor)+"\":\""+str(window_sensor)+"\",\"FLOW\":{\"publish\":"+str(publish_msg)+",\"collect\":"+str(collect_msg)+"}}}"
			print(a)
			client.publish('dev/'+args.name,a)
			window_sensor = []
			
			#Obter os dados recebidos da mensagem TATU considerando que a mensagem esta na variavel 'a'
			#obj=to_object(a)
			
			#Nome do sensor
			#print(obj.HEADER['NAME'])
			
			#Valor do conecpt Drift como Boolean e não como string
			#print(eval(obj.BODY['conceptDrift']))
			
			#retorna a janela de dados em formato vetor e não como string
			#window_data=obj.BODY['temperatureSensor'].replace('[','').replace(']','').split(',')
			#print(window_data[0])
			#print(window_data)

def config_publish_collect(st,name):
	global publish_msg,collect_msg, args
	print ("Init storage flow")
	st=st.replace('FLOW INFO '+args.sensor,'')
	print("c0")
	st=st.replace("collect","\"collect\"")
	st=st.replace("publish","\"publish\"")
	print("c1")
	name=name.replace('dev/','')
	#st=st.replace(' ','')
	#print(st)
	#ob=to_object(st)
	print("c2")
	#publish_msg=int(ob.publish)
	#collect_msg=int(ob.collect)
	publish_msg=4000
	collect_msg=4000	
	#print("chama 2",publish_msg," complemento",collect_msg)
		
def catch_message(msg,topic):
	global thread_use, tatu_message_type, args, dataSet
	#print(msg)
	print(msg.find('FLOW INFO '+args.sensor+' {collect'))
	if(msg.find('FLOW INFO '+args.sensor+' {collect')!=-1 and topic.find('dev/'+args.name)!=-1):
		#iniciar flow via thread
		print('chama')
		try:		
			tatu_message_type="flow"
			print(tatu_message_type)
			thread_use=True
			config_publish_collect(msg,args.name)
			a = Th()
			a.daemon=True
			a.start()
		except Exception as inst:
			print(inst)
			logging.exception(str(inst))
	elif(msg.find('FLOW INFO '+args.sensor+' {turn:1}')==0 and topic.find('dev/'+name_device)==0):
		#finalizar flow
		thread_use=False
		


def on_connect(client, userdata, flags, rc):
	print('conectou')
	global topicSubscribe, args
    #automatic subscribe
	print('conectou')
	client.subscribe(topicSubscribe)
	
	
#Broker receiver
def on_message(client, userdata, msg):
	mensagemRecebida = str(msg.payload)
	#chegou msg
	print ("On Message: ",mensagemRecebida," topic: ",msg.topic)
	catch_message(mensagemRecebida,str(msg.topic))
	
	
def config_mqtt():
	global client, dataSet
	try:	
		print("Init Virtual Device")
		#estatico para todos sensores
		print(direc)
		print(portBroker)
		print("Sensor name: "+str(sensorName))
		#dataSet = data_set.DataSetReader(direc)
		#print("data set open:  "+ str(dataSet.next_value(sensorName))
		client.on_connect = on_connect
		client.on_message = on_message
		print('Connect')

		client.connect(broker, int(portBroker), keepAliveBroker)
		client.loop_forever()
	except Exception as inst:
		print(inst)
		logging.exception(str(inst))
		print ("\nCtrl+C leav...")
		sys.exit(0)
			
###########Main
config_mqtt()
