'''
Program to intercept, manage, and re-post MQTT messages for the 
lightning detector and rain gauge.
mqtt documentation is at https://pypi.org/project/paho-mqtt/#callbacks
121919dep
'''

import time
import threading
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

######### Constants
MQTT_USER='mqtt'
MQTT_PASS='tinymqtt'
HASS_HOST = "192.168.1.99"

IN_LIGHTNING_TOPIC="rtl_433/tinyserver/devices/Acurite-6045M/A/+/strike_count"
OUT_LIGHTNING_TOPIC="rtl_433/tinyserver/devices/Acurite-6045M/A/106/~period~/modified_strike_count"
IN_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/+/rain_mm"
HOURLY_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/~period~/hourly_rain_mm"
DAILY_RAIN_TOPIC="rtl_433/tinyserver/devices/Acurite-Rain899/0/80/~period~/daily_rain_mm"
IN_OUTDOOR_TEMPERATURE_TOPIC="rtl_433/tinyserver/devices/Acurite-Tower/A/+/temperature_C"
OUT_OUTDOOR_TEMPERATURE_TOPIC="rtl_433/tinyserver/devices/Acurite-Tower/A/calc/temperature_F"
PIR_SENSOR_TOPIC="rtl_433/tinyserver/devices/PIR_sensor/count"

LIGHTNING_PERIOD=10*60 		# seconds
RAIN_HOURLY_PERIOD=60*60	# seconds
RAIN_DAILY_PERIOD=RAIN_HOURLY_PERIOD*24
RAIN_WEEKLY_PERIOD=RAIN_DAILY_PERIOD*7
RAIN_MONTHLY_PERIOD=RAIN_DAILY_PERIOD*30

PIR_DELAY=60 # seconds

######### Globals
strikeHistory=[]
rainHistory=[]
pirThreads=[]

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
	publish(topic,value)

def publishDailyRain(period,value):
	topic=DAILY_RAIN_TOPIC.replace("~period~",str(period))
	publish(topic,value)

def publishOutdoorTemperature(value):
	publish(OUT_OUTDOOR_TEMPERATURE_TOPIC,value)

def absentThread(): # to publish "no motion" after a period of time
	publish(PIR_SENSOR_TOPIC,0)

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
	now = round(int(time.time()),-1)  # right now to the nearest 10 secs
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
			publishLightning(LIGHTNING_PERIOD, entry.value-strk.value)
			break


# This is the main routine for the rain sensor.
# It is called for each MQTT message received that contains
# the rain amount. The rain amount is cumulative, so it needs
# to be broken down by time into short periods.  The period length
# is defined by the constants RAIN_*_PERIOD above.
def onRainMessage(client, userdata, message):
	now = round(int(time.time()),-1)  # right now to the nearest 10 secs

	# Get rid of extra digits to the right of decimal
	value=round(float(message.payload),2)

	entry=timeval(now,value)
	if entry in rainHistory:
		return
	
	rainHistory.append(entry) # add it to the list

	# iterate through the list, and calculate the rainfall in 
	# the last RAIN_HOURLY_PERIOD seconds.  Remove any from the list
	# that are not in that period. Later we will modify this to give
	# daily, weekly, and monthly values as well.
	dailyDone=False
	for rain in rainHistory:
		if rain.time < now - RAIN_DAILY_PERIOD:
			rainHistory.remove(rain)
		elif rain.time >= now - RAIN_HOURLY_PERIOD:
			publishHourlyRain(RAIN_HOURLY_PERIOD, round(value-rain.value,2))
			break
		elif not(dailyDone):
			dailyDone=True
			publishDailyRain(RAIN_DAILY_PERIOD, round(value-rain.value,2))
	if not(dailyDone): # Publish daily number even if we haven't run long enough
		publishDailyRain(RAIN_DAILY_PERIOD, round(value-rain.value,2))

# This intercepts the outdoor temperature in Celcius and 
# converts it to Fahrenheit
def onOutdoorTemperatureMessage(client, userdata, message):
	value=float(message.payload)*9/5 +32.0
	publishOutdoorTemperature(round(value,1))

def on_log(client, userdata, level, buf):
    print("log: ",buf)

def onSubscribe(client, userdata, mid, granted_qos):
	print(str(userdata))

def onPIR(client, userdata, message):
	# Cancel any pending reports
	global pirThreads
	for t in pirThreads:
		t.cancel()
	
	# Start a new one
	if (int(message.payload)==1):
		pirTh=threading.Timer(PIR_DELAY, absentThread)
		pirThreads.append(pirTh)
		pirTh.start()

########### Processing begins here
client = mqtt.Client()
client.username_pw_set(MQTT_USER, password=MQTT_PASS)
client.on_connect = onConnect
client.message_callback_add(IN_LIGHTNING_TOPIC, onLightningMessage)
client.message_callback_add(IN_RAIN_TOPIC, onRainMessage)
client.message_callback_add(IN_OUTDOOR_TEMPERATURE_TOPIC, onOutdoorTemperatureMessage)
client.message_callback_add(PIR_SENSOR_TOPIC, onPIR)

print("Connecting client to HA's MQTT broker")
client.connect(HASS_HOST, 1883, 60)
client.subscribe([(IN_LIGHTNING_TOPIC, 0), (IN_RAIN_TOPIC, 0), (IN_OUTDOOR_TEMPERATURE_TOPIC, 0), (PIR_SENSOR_TOPIC, 0)])


client.loop_forever(timeout=1.0)

