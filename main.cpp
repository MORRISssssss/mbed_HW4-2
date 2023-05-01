#include "mbed.h"
#include "MQTTNetwork.h"
#include "MQTTmbed.h"
#include "MQTTClient.h"
#include "accelerometer.h"

// GLOBAL VARIABLES
WiFiInterface *wifi;
DigitalOut led(LED1);
volatile int message_num = 0;
volatile int arrivedcount = 0;
volatile bool closed = false;

const char* topic = "Mbed";
Accelerometer acc;
double acc_data[3] = {0};

Thread mqtt_thread(osPriorityHigh);
EventQueue mqtt_queue(256 * EVENTS_EVENT_SIZE);

void messageArrived(MQTT::MessageData& md) {
    MQTT::Message &message = md.message;
    char msg[300];
    sprintf(msg, "Message arrived: QoS%d, retained %d, dup %d, packetID %d\r\n", message.qos, message.retained, message.dup, message.id);
    //printf(msg);
    //ThisThread::sleep_for(2000ms);
    char payload[300];
    sprintf(payload, "Payload %.*s\r\n", message.payloadlen, (char*)message.payload);
    //printf(payload);
    ++arrivedcount;
}

void publish_message(MQTT::Client<MQTTNetwork, Countdown>* client, double* acc_data) {
    message_num++;
    MQTT::Message message;
    char buff[100];
    sprintf(buff, "%lf/%lf/%lf\r\n", acc_data[0], acc_data[1], acc_data[2]);
    message.qos = MQTT::QOS0;
    message.retained = false;
    message.dup = false;
    message.payload = (void*) buff;
    message.payloadlen = strlen(buff) + 1;
    int rc = client->publish(topic, message);
    led = !led;

    //printf("rc:  %d\r\n", rc);
    //printf("Puslish message: %s\r\n", buff);
}

void close_mqtt() {
    closed = true;
}

int main() {

    wifi = WiFiInterface::get_default_instance();
    if (!wifi) {
            printf("ERROR: No WiFiInterface found.\r\n");
            return -1;
    }


    printf("\nConnecting to %s...\r\n", MBED_CONF_APP_WIFI_SSID);
    int ret = wifi->connect(MBED_CONF_APP_WIFI_SSID, MBED_CONF_APP_WIFI_PASSWORD, NSAPI_SECURITY_WPA_WPA2);
    if (ret != 0) {
            printf("\nConnection error: %d\r\n", ret);
            return -1;
    }


    NetworkInterface* net = wifi;
    MQTTNetwork mqttNetwork(net);
    MQTT::Client<MQTTNetwork, Countdown> client(mqttNetwork);

    //TODO: revise host to your IP
    const char* host = "140.114.207.93";
    const int port=1883;
    printf("Connecting to TCP network...\r\n");
    printf("address is %s/%d\r\n", host, port);

    int rc = mqttNetwork.connect(host, port);//(host, 1883);
    if (rc != 0) {
            printf("Connection error.");
            return -1;
    }
    printf("Successfully connected!\r\n");

    MQTTPacket_connectData data = MQTTPacket_connectData_initializer;
    data.MQTTVersion = 3;
    data.clientID.cstring = "Mbed";

    if ((rc = client.connect(data)) != 0){
            printf("Fail to connect MQTT\r\n");
    }
    if (client.subscribe(topic, MQTT::QOS0, messageArrived) != 0){
            printf("Fail to subscribe\r\n");
    }

    mqtt_thread.start(callback(&mqtt_queue, &EventQueue::dispatch_forever));

    while (1) {
        if (closed) break;
        acc.GetAcceleromterSensor(acc_data);
        acc.GetAcceleromterCalibratedData(acc_data);
        //printf("%lf/%lf/%lf\n", acc_data[0], acc_data[1], acc_data[2]);
        mqtt_queue.call(publish_message, &client, acc_data);
        client.yield(100);
        ThisThread::sleep_for(100ms);
    }

    printf("Ready to close MQTT Network......\n");

    if ((rc = client.unsubscribe(topic)) != 0) {
        printf("Failed: rc from unsubscribe was %d\n", rc);
    }
    if ((rc = client.disconnect()) != 0) {
    printf("Failed: rc from disconnect was %d\n", rc);
    }

    mqttNetwork.disconnect();
    printf("Successfully closed!\n");

    return 0;
}