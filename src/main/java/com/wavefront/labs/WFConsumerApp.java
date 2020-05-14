package com.wavefront.labs;

import java.time.Duration;

/*
 * Original code from: https://github.com/garg-geek/kafka
 * Great work
 */

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.wavefront.labs.IKafkaConstants;
import com.wavefront.labs.sender.Wavefront;
import com.wavefront.labs.ConsumerCreator;

import java.io.IOException;

public class WFConsumerApp {

    public static Wavefront wavefront = null;
    public static String KAFKA_BROKERS = "KafkaServer.address:9092";
    public static String CLIENT_ID="AnyClientNameThatWorks";
    public static String TOPIC_NAME="TopicNameHere";
    public static String GROUP_ID="SomeGroupToTrackLogically";

    public static void main(String[] args) {

        getArgs(args);
        runConsumer();

    }

    private static void getArgs(String[] args) {
        if(args.length < 2) {
            printUsage();
            return;
        }

        for(int x = 0;x < args.length;x++)	{
            if(args[x] != null && args[x].startsWith("-b")) {
                KAFKA_BROKERS = x==args.length ? args[x]:args[x + 1];
            }
            if(args[x] != null && args[x].startsWith("-c")) {
                CLIENT_ID = x==args.length ? args[x]:args[x + 1];
            }
            if(args[x] != null && args[x].startsWith("-t")) {
                TOPIC_NAME = x==args.length ? args[x]:args[x + 1];
            }
            if(args[x] != null && args[x].startsWith("-g")) {
                GROUP_ID = x==args.length ? args[x]:args[x + 1];
            }
        }
    }

    private static void printUsage() {
        System.out.println("Incorrect syntax! Example Usage:\n");
        System.out.println("java -jar wfkafka.jar -b 192.168.0.1:9092 -c client1 -t topicname");

    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(KAFKA_BROKERS, GROUP_ID, CLIENT_ID, TOPIC_NAME);

        int noMessageToFetch = 0;

		/*
		try {
			//Construct basic Wavefront, specifying the proxy and port.
			wavefront = new Wavefront("192.168.1.5", 2878);
		} catch (IOException e) {
			System.out.println("Error connecting to WF Proxy!");
			e.printStackTrace();
		}
		*/

        int iter = 0 ;
        while (true) {

            try {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

                if (consumerRecords.count() == 0 || consumerRecords.isEmpty()) {
                    noMessageToFetch++;
                    if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                        break;
                    else
                        continue;
                }

                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());

                    //WF sending
                    //String tags = "\"test\"=\"true\" \"tester\"=Steve";
                    //wavefront.send(record.key() + "." + "TestKafkaMetric", record.value(), "testHost", 0, tags);
                });
                System.out.println("Here at " + iter);
                consumer.commitAsync();
                iter++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        consumer.close();
    }


}
