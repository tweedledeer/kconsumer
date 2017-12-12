import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Properties;

/**
 * Author gkarabut
 * since 12/1/17.
 */

public class KafkaConsumerExample {
  public static final String PROPSFILE = "kafka.properties";

    private static Properties getKafkaProperties(){
      Properties props = new Properties();
      String filename = PROPSFILE;
      InputStream inputStream = KafkaConsumerExample.class.getResourceAsStream(filename);
      try{
        props.load(inputStream);}
      catch (Exception ex){
        System.out.println("Exception thrown on getting properties from " + filename + ": " + ex.getMessage());
        return props;
        }
      return props;
    }

  private static org.apache.kafka.clients.consumer.Consumer<Long, String> createConsumer() {
    final Properties props = new Properties();
    try {
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaProperties().getProperty("bootstrapEndpoint"));
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }catch (Exception ex){
      System.out.println("Exception thrown on setting consumer properties: " + ex.getMessage()+ ". Most likely"
          + " properties file was not found: " + PROPSFILE);
    }

    // Create the consumer using props.
    final Consumer<Long, String> consumer =
        new KafkaConsumer<>(props);

    // Subscribe to the topic.
    consumer.subscribe(Collections.singletonList(getKafkaProperties().getProperty("kafkaTopic")));
    return consumer;
  }


  static void runConsumer() {
    final Consumer<Long, String> consumer = createConsumer();
    PrintWriter writer;
    try {
      writer = new PrintWriter("output.log", "UTF-8");
    }catch  (Exception ex){
      writer = null;
    }

    final int giveUp = 10000;   int noRecordsCount = 0;

    while (true) {
      final ConsumerRecords<Long, String> consumerRecords =
          consumer.poll(1000);

      if (consumerRecords.count()==0) {
        noRecordsCount++;
        if (noRecordsCount > giveUp) break;
        else continue;
      }
      consumerRecords.forEach(record -> {
        final String record1 = String.format("Consumer Record:(%d, %s, %d, %d)\n",
            record.key(), record.value(),
            record.partition(), record.offset());
        System.out.printf(record1);
      });
      writer.println(consumerRecords);

      consumer.commitAsync();
    }
    writer.close();
    consumer.close();
    System.out.println("DONE");
  }

  public static void main (String[] args) throws  Exception{
      runConsumer();
  }


}
