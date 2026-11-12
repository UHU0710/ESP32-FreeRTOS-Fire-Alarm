#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <math.h>

// GPIO theo mach Wokwi da kiem tra
#define DHT_PIN 15
#define MQ2_PIN 34
#define FAN_PIN 25
#define PUMP_PIN 26
#define GAS_THRESHOLD 2000
#define TEMP_THRESHOLD 45.0f

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";
const char* MQTT_SERVER = "test.mosquitto.org";
const int MQTT_PORT = 1883;
// Phai giong topic goc trong index.html
const char* TOPIC_DATA = "lab3/huubui/fire01/data";
const char* TOPIC_STATE = "lab3/huubui/fire01/state";
const char* TOPIC_COMMAND = "lab3/huubui/fire01/command";

DHTesp dht;
WiFiClient net;
PubSubClient mqtt(net);

struct SensorData {
  int gas;
  float temperature;
  float humidity;
  bool validTemp;
};
QueueHandle_t dataQueue = nullptr;
QueueHandle_t dataAlert = nullptr;

// Du lieu dieu khien chia se giua hai core: bao ve bang mutex
SemaphoreHandle_t controlMutex = nullptr;
struct ControlState {
  bool manual;
  bool manualFan;
  bool manualPump;
  bool fan;
  bool pump;
} control = {false, false, false, false, false};

void publishState() { // Chi goi tu TaskSendData - Core 0
  ControlState s;
  if (xSemaphoreTake(controlMutex, pdMS_TO_TICKS(100)) != pdTRUE) return;
  s = control;
  xSemaphoreGive(controlMutex);
  char payload[140];
  snprintf(payload, sizeof(payload),
    "{\"mode\":\"%s\",\"fan\":%s,\"pump\":%s}",
    s.manual ? "manual" : "auto", s.fan ? "true" : "false", s.pump ? "true" : "false");
  mqtt.publish(TOPIC_STATE, payload, true);
}

// Callback MQTT chay trong TaskSendData - Core 0
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, TOPIC_COMMAND) != 0) return;
  // Parse cac lenh JSON co dinh: {"mode":"manual"}, {"mode":"auto"}, {"fan":true/false}, {"pump":true/false}
  if (length == 0 || length >= 180) return;
  char command[180];
  memcpy(command, payload, length);
  command[length] = '\0';
  String msg(command);
  msg.replace(" ", "");
  msg.replace("\n", "");
  bool changed = false;
  if (xSemaphoreTake(controlMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    if (msg.indexOf("\"mode\":\"manual\"") >= 0) {
      control.manual = true; changed = true;
    } else if (msg.indexOf("\"mode\":\"auto\"") >= 0) {
      control.manual = false; changed = true;
    } else if (control.manual) {
      if (msg.indexOf("\"fan\":true") >= 0) { control.manualFan = true; changed = true; }
      else if (msg.indexOf("\"fan\":false") >= 0) { control.manualFan = false; changed = true; }
      if (msg.indexOf("\"pump\":true") >= 0) { control.manualPump = true; changed = true; }
      else if (msg.indexOf("\"pump\":false") >= 0) { control.manualPump = false; changed = true; }
    }
    xSemaphoreGive(controlMutex);
  }
  if (changed) {
    Serial.print("[MQTT CMD] "); Serial.println(command);
    // Cap nhat trang thai ngay khi co lenh (TaskAlert se doc va ap dung trong toi da 100ms)
  }
}

void TaskReadData(void* pv) { // Core 1
  SensorData d;
  for (;;) {
    d.gas = analogRead(MQ2_PIN);
    auto r = dht.getTempAndHumidity();
    d.temperature = r.temperature;
    d.humidity = r.humidity;
    d.validTemp = dht.getStatus() == DHTesp::ERROR_NONE && !isnan(d.temperature) && !isnan(d.humidity);
    Serial.printf("[READ] Gas ADC=%d | Temp=", d.gas);
    if (d.validTemp) Serial.printf("%.2f C | Humidity=%.2f%%\n", d.temperature, d.humidity);
    else Serial.println("ERROR DHT22");
    xQueueOverwrite(dataQueue, &d);
    xQueueOverwrite(dataAlert, &d);
    vTaskDelay(pdMS_TO_TICKS(2500));
  }
}

void TaskAlert(void* pv) { // Core 1
  SensorData latest = {0, NAN, NAN, false};
  bool haveData = false;
  bool prevFan = false, prevPump = false;
  for (;;) {
    SensorData received;
    if (xQueueReceive(dataAlert, &received, pdMS_TO_TICKS(100)) == pdTRUE) {
      latest = received;
      haveData = true;
    }
    bool fan = false, pump = false;
    if (xSemaphoreTake(controlMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
      if (control.manual) {
        fan = control.manualFan;
        pump = control.manualPump;
      } else if (haveData) {
        fan = latest.gas > GAS_THRESHOLD;
        pump = latest.validTemp && latest.temperature > TEMP_THRESHOLD;
      }
      control.fan = fan;
      control.pump = pump;
      xSemaphoreGive(controlMutex);
    }
    digitalWrite(FAN_PIN, fan ? HIGH : LOW);
    digitalWrite(PUMP_PIN, pump ? HIGH : LOW);
    if (fan != prevFan || pump != prevPump) {
      Serial.printf("[ALERT] FAN=%s | PUMP=%s\n", fan ? "ON" : "OFF", pump ? "ON" : "OFF");
      prevFan = fan; prevPump = pump;
    }
  }
}

void TaskSendData(void* pv) { // Core 0
  mqtt.setServer(MQTT_SERVER, MQTT_PORT);
  mqtt.setCallback(mqttCallback);
  mqtt.setBufferSize(512);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS, 6);
  unsigned long lastWiFiTry = 0, lastMqttTry = 0, lastStatePublish = 0;
  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      if (millis() - lastWiFiTry > 5000) {
        lastWiFiTry = millis(); WiFi.reconnect();
        Serial.println("[WIFI] Connecting...");
      }
      vTaskDelay(pdMS_TO_TICKS(100)); continue;
    }
    if (!mqtt.connected()) {
      if (millis() - lastMqttTry > 5000) {
        lastMqttTry = millis();
        String id = "esp32-fire-" + String((uint32_t)esp_random(), HEX);
        if (mqtt.connect(id.c_str())) {
          Serial.println("[MQTT] CONNECTED");
          mqtt.subscribe(TOPIC_COMMAND);
          publishState();
        } else Serial.printf("[MQTT] Connection failed: %d\n", mqtt.state());
      }
      vTaskDelay(pdMS_TO_TICKS(100)); continue;
    }
    mqtt.loop();
    SensorData d;
    if (xQueueReceive(dataQueue, &d, 0) == pdTRUE) {
      ControlState s;
      if (xSemaphoreTake(controlMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        s = control;
        xSemaphoreGive(controlMutex);
      } else s = {false,false,false,false,false};
      char temp[20], humidity[20], payload[256];
      if (d.validTemp) {
        snprintf(temp, sizeof(temp), "%.2f", d.temperature);
        snprintf(humidity, sizeof(humidity), "%.2f", d.humidity);
      } else { strcpy(temp, "null"); strcpy(humidity, "null"); }
      snprintf(payload, sizeof(payload),
        "{\"gas\":%d,\"temperature\":%s,\"humidity\":%s,\"fan\":%s,\"pump\":%s}",
        d.gas, temp, humidity, s.fan?"true":"false", s.pump?"true":"false");
      bool ok = mqtt.publish(TOPIC_DATA, payload);
      Serial.printf("[MQTT] %s %s\n", ok ? "SENT:" : "FAILED:", payload);
    }
    if (millis() - lastStatePublish >= 1000) {
      lastStatePublish = millis();
      publishState();
    }
    vTaskDelay(pdMS_TO_TICKS(50));
  }
}

void setup() {
  Serial.begin(115200);
  pinMode(MQ2_PIN, INPUT);
  pinMode(FAN_PIN, OUTPUT); pinMode(PUMP_PIN, OUTPUT);
  digitalWrite(FAN_PIN, LOW); digitalWrite(PUMP_PIN, LOW);
  dht.setup(DHT_PIN, DHTesp::DHT22);
  controlMutex = xSemaphoreCreateMutex();
  dataQueue = xQueueCreate(1, sizeof(SensorData));
  dataAlert = xQueueCreate(1, sizeof(SensorData));
  if (!controlMutex || !dataQueue || !dataAlert) {
    Serial.println("ERROR: Cannot create RTOS queues/mutex");
    return;
  }
  xTaskCreatePinnedToCore(TaskReadData, "ReadData", 4096, NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(TaskAlert, "Alert", 4096, NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(TaskSendData, "SendData", 8192, NULL, 1, NULL, 0);
  Serial.println("FreeRTOS + MQTT control started");
}
void loop() { vTaskDelay(pdMS_TO_TICKS(1000)); }
