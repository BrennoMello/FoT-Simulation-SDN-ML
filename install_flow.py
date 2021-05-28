import argparse 
import sys
sys.path.insert(0, '/home/mininet/projeto_ml/FoT-Stream_Simulation/reg')
import utils_hosts
import os

parser = argparse.ArgumentParser(prog='req_gateway', usage='%(prog)s [options]', description='Communication Gateway-Sensor')
parser.add_argument('-s','--sensorip', type=str, help='Sensor-address IP',required=False)
parser.add_argument('-g','--gatewayip', type=str, help='Gateways-address IP',required=False)
parser.add_argument('-t','--time', type=str, help='Time of Timeout',required=False)

args = parser.parse_args()
#max_switches=utils_hosts.return_qtd_switches()
max_switches=7
for i in range(0,max_switches):
	if(args.time==None):
		os.system('ovs-ofctl add-flow s'+str(i+1)+' hard_timeout=15,ip,nw_src='+args.sensorip+',nw_dst='+args.gatewayip+',actions=normal')
		os.system('ovs-ofctl add-flow s'+str(i+1)+' hard_timeout=15,ip,nw_src='+args.gatewayip+',nw_dst='+args.sensorip+',actions=normal')
	else:
		os.system('ovs-ofctl add-flow s'+str(i+1)+' hard_timeout='+str(args.time)+',ip,nw_src='+args.sensorip+',nw_dst='+args.gatewayip+',actions=normal')
		os.system('ovs-ofctl add-flow s'+str(i+1)+' hard_timeout='+str(args.time)+',ip,nw_src='+args.gatewayip+',nw_dst='+args.sensorip+',actions=normal')
	#print 'ovs-ofctl add-flow s'+str(i+1)+' idle_timeout=10,ip,nw_src='+args.sensorip+',nw_dst='+args.gatewayip+',actions=normal' 
