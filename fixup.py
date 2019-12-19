'''
Program to intercept, manage, and re-post MQTT messages for the 
lightning detector and rain gauge.
mqtt documentation is at https://pypi.org/project/paho-mqtt/#callbacks
121919dep
'''

import time
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

######### Constants
MQTT_USER='mqtt'
MQTT_PASS='tinymqtt'
HASS_HOST = "192.168.1.99"

IN_LIGHTNING_TOPIC="rtl_433/tinyserver/devices/Acurite-6045M/A/106/strike_count"
OUT_LIGHTNING_TOPIC="rtl_433/tinyserver/devices/Acurite-6045M/A/106/~period~/modified_strike_count"
RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/rain_mm"

LIGHTNING_PERIOD=10*60 # seconds

######### Globals
strikeHistory=[]

######### Classes
class strike:
	time=0
	count=0
	def __init__(self,time,count):
		self.time=time
		self.count=count

######### Local Functions
def publishLightning(value):
	topic=OUT_LIGHTNING_TOPIC.replace("~period~",str(LIGHTNING_PERIOD))
	print("publishing "+str(value)+" to "+topic)
	rc=client.publish(topic, payload=value, qos=0, retain=False)
	if rc.rc != mqtt.MQTT_ERR_SUCCESS:
		print("Publish failed! rc="+str(rc))

######### Callbacks
def onConnect(client, userdata, flags, rc):
	print("Client connected!")
	
	
# This is the main routine for the lightning strikes.
# It is called for each MQTT message received that contains
# the strike count. The strike count is cumulative, so it needs
# to be broken down by time into short periods.  The period length
# is defined by the constant LIGHTNING_PERIOD above.
def onLightningMessage(client, userdata, message):
	now = int(time.time())  # right now
	strikeHistory.append(strike(now,int(message.payload))) # add it to the list
	
	# iterate through the list, and calculate the strikes in 
	# the last LIGHTNING_PERIOD seconds.  Remove any from the list
	# that are not in that period.
	for strk in strikeHistory:
		if strk.time < now - LIGHTNING_PERIOD:
			strikeHistory.remove(strk)
		else:
			publishLightning(int(message.payload)-strk.count)
			break



def onRainMessage(client, userdata, message):
	print(float(message.payload))
	
def on_log(client, userdata, level, buf):
    print("log: ",buf)

def onSubscribe(client, userdata, mid, granted_qos):
	print(str(userdata))

client = mqtt.Client()
client.username_pw_set(MQTT_USER, password=MQTT_PASS)
client.on_connect = onConnect
client.message_callback_add(IN_LIGHTNING_TOPIC, onLightningMessage)
client.message_callback_add(RAIN_TOPIC, onRainMessage)

print("Connecting client to HA's MQTT broker")
client.connect(HASS_HOST, 1883, 60)
client.subscribe([(IN_LIGHTNING_TOPIC, 0), (RAIN_TOPIC, 0)])


client.loop_forever(timeout=1.0)

