/*
 * Bài thực hành 2 – phát triển từ Bài 1 (ESP32 + DHT22 + HC-SR04 + LED trên Wokwi)
 * Khác Bài 1: không gửi lên ThingsBoard mà publish lên MQTT broker công cộng, để collector Python
 * trên máy cá nhân subscribe, kiểm tra và lưu vào InfluxDB.
 * Thêm: timestamp NTP (ts, ms) để đo độ trễ, device_id, chu kỳ 2 giây, Last Will, tự kết nối lại.
 * Giữ nguyên các khóa của Bài 1: temperature, humidity, distance_cm, rssi, sequence, uptime_s.
 * Thư viện (libraries.txt): "DHT sensor library for ESPx", "PubSubClient"
 */
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <sys/time.h>

const char* WIFI_SSID     = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

const char* MQTT_SERVER = "broker.hivemq.com";   // broker công cộng (không bảo mật) – chỉ dùng cho thực hành
const int   MQTT_PORT   = 1883;
const char* SITE        = "b23dccn847";          // ĐỔI thành chuỗi riêng của bạn để tránh trùng topic với người khác
const char* DEVICE_ID   = "esp32-01";
const unsigned long SEND_INTERVAL_MS = 2000;

#define DHT_PIN  15
#define TRIG_PIN 5
#define ECHO_PIN 18
#define LED_PIN  2

DHTesp dht;
WiFiClient espClient;
PubSubClient client(espClient);

char topicTelemetry[80];
char topicStatus[80];
char clientId[48];
unsigned long lastSend = 0;
unsigned long sequence = 0;

int64_t nowMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (int64_t)tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

void connectWiFi() {
  Serial.print("Dang ket noi WiFi");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  Serial.print("Da ket noi WiFi, IP: ");
  Serial.println(WiFi.localIP());
  configTime(0, 0, "pool.ntp.org", "time.google.com");   // đồng bộ giờ để đo độ trễ
  unsigned long t0 = millis();
  while (nowMs() < 1600000000000LL && millis() - t0 < 15000) delay(200);
  Serial.println(nowMs() > 1600000000000LL ? "NTP OK" : "NTP chua dong bo");
}

void connectMQTT() {
  while (!client.connected()) {
    Serial.print("Dang ket noi MQTT...");
    // Last Will: broker tự đăng "offline" khi ESP32 mất kết nối đột ngột
    if (client.connect(clientId, nullptr, nullptr, topicStatus, 1, true, "offline")) {
      Serial.println(" MQTT connected!");
      client.publish(topicStatus, "online", true);
    } else {
      Serial.print(" that bai, rc=");
      Serial.println(client.state());
      if (WiFi.status() != WL_CONNECTED) connectWiFi();
      delay(2000);
    }
  }
}

// Trả về khoảng cách (cm) hoặc -1 nếu timeout / ngoài miền 2–400 cm
float readDistance() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  long duration = pulseIn(ECHO_PIN, HIGH, 30000);
  if (duration == 0) return -1;
  float distance = duration * 0.0343 / 2;
  if (distance < 2 || distance > 400) return -1;
  return distance;
}

void setup() {
  Serial.begin(115200);
  dht.setup(DHT_PIN, DHTesp::DHT22);
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);

  snprintf(topicTelemetry, sizeof(topicTelemetry), "iot/%s/%s/telemetry", SITE, DEVICE_ID);
  snprintf(topicStatus, sizeof(topicStatus), "iot/%s/%s/status", SITE, DEVICE_ID);
  snprintf(clientId, sizeof(clientId), "%s-%s-%04x", SITE, DEVICE_ID, (unsigned)(esp_random() & 0xFFFF));

  connectWiFi();
  client.setServer(MQTT_SERVER, MQTT_PORT);
  client.setBufferSize(512);          // mặc định 256 B là không đủ cho payload này
  client.setKeepAlive(15);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!client.connected()) connectMQTT();
  client.loop();

  if (millis() - lastSend >= SEND_INTERVAL_MS) {
    lastSend = millis();

    TempAndHumidity data = dht.getTempAndHumidity();
    if (isnan(data.temperature) || isnan(data.humidity)) {
      Serial.println("Du lieu cam bien khong hop le - bo qua");
      return;                          // không gửi gói sai (giống Bài 1)
    }

    float distance = readDistance();
    sequence++;

    char distPart[32] = "";
    if (distance >= 0) snprintf(distPart, sizeof(distPart), ",\"distance_cm\":%.2f", distance);

    char payload[256];
    snprintf(payload, sizeof(payload),
             "{\"device_id\":\"%s\",\"ts\":%lld,\"sequence\":%lu,\"temperature\":%.2f,\"humidity\":%.2f%s,\"rssi\":%d,\"uptime_s\":%lu}",
             DEVICE_ID, (long long)nowMs(), sequence, data.temperature, data.humidity, distPart,
             WiFi.RSSI(), millis() / 1000);

    Serial.println(payload);
    if (client.publish(topicTelemetry, payload)) {
      digitalWrite(LED_PIN, HIGH);     // LED nháy khi gửi thành công (như Bài 1)
      delay(100);
      digitalWrite(LED_PIN, LOW);
      Serial.println("Gui thanh cong");
    } else {
      Serial.println("Gui that bai");
    }
  }
}
