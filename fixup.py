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
IN_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/rain_mm"
HOURLY_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/~period~/hourly_rain_mm"
DAILY_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/~period~/daily_rain_mm"

LIGHTNING_PERIOD=10*60 		# seconds
RAIN_HOURLY_PERIOD=60*60	# seconds
RAIN_DAILY_PERIOD=RAIN_HOURLY_PERIOD*24
RAIN_WEEKLY_PERIOD=RAIN_DAILY_PERIOD*7
RAIN_MONTHLY_PERIOD=RAIN_DAILY_PERIOD*30


######### Globals
strikeHistory=[]
rainHistory=[]

######### Classes
class timeval:
	time=0
	value=0
	def __init__(self,time,value):
		self.time=time
		self.value=value
	def __eq__(self, other):
		return self.time == other.time and self.value == other.value
	def __ne__(self, other):
		return not(self.__eq__(self, other))

######### Local Functions
def publishLightning(period,value):
	topic=OUT_LIGHTNING_TOPIC.replace("~period~",str(period))
	publish(topic,value)

def publishHourlyRain(period,value):
	topic=HOURLY_RAIN_TOPIC.replace("~period~",str(period))
	# Get rid of extra digits to the right of decimal
	txt="{rain:.2f}"
	value=float(txt.format(rain=value))
	publish(topic,value)

def publish(topic,value):
	print("publishing "+str(value)+" to "+topic+" ("
	+str(len(strikeHistory))+"/"+str(len(rainHistory))+")")
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
	entry=timeval(now,int(message.payload))
	if entry in strikeHistory:
		return
		
	strikeHistory.append(entry) # add it to the list
	
	# iterate through the list, and calculate the strikes in 
	# the last LIGHTNING_PERIOD seconds.  Remove any from the list
	# that are not in that period.
	for strk in strikeHistory:
		if strk.time < now - LIGHTNING_PERIOD:
			strikeHistory.remove(strk)
		else:
			publishLightning(LIGHTNING_PERIOD, int(message.payload)-strk.value)
			break


# This is the main routine for the rain sensor.
# It is called for each MQTT message received that contains
# the rain amount. The rain amount is cumulative, so it needs
# to be broken down by time into short periods.  The period length
# is defined by the constants RAIN_*_PERIOD above.
def onRainMessage(client, userdata, message):
	now = int(time.time())  # right now
	entry=timeval(now,float(message.payload))
	if entry in rainHistory:
		return
	
	rainHistory.append(entry) # add it to the list

	# iterate through the list, and calculate the rainfall in 
	# the last RAIN_HOURLY_PERIOD seconds.  Remove any from the list
	# that are not in that period. Later we will modify this to give
	# daily, weekly, and monthly values as well.
	for rain in rainHistory:
		if rain.time < now - RAIN_HOURLY_PERIOD:
			rainHistory.remove(rain)
		else:
			publishHourlyRain(RAIN_HOURLY_PERIOD, float(message.payload)-rain.value)
			break
	

def on_log(client, userdata, level, buf):
    print("log: ",buf)

def onSubscribe(client, userdata, mid, granted_qos):
	print(str(userdata))

client = mqtt.Client()
client.username_pw_set(MQTT_USER, password=MQTT_PASS)
client.on_connect = onConnect
client.message_callback_add(IN_LIGHTNING_TOPIC, onLightningMessage)
client.message_callback_add(IN_RAIN_TOPIC, onRainMessage)

print("Connecting client to HA's MQTT broker")
client.connect(HASS_HOST, 1883, 60)
client.subscribe([(IN_LIGHTNING_TOPIC, 0), (IN_RAIN_TOPIC, 0)])


client.loop_forever(timeout=1.0)

