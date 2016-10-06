
//import util.properties packages
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//Create java class named “SimpleProducer”
public class SimpleProducer {

	public static String TOPIC;
	public static String BOOTSTRAP_SERVER;

	public static void main(String[] args) {

		// Check arguments length value
		if (args.length < 2) {
			System.out.println("Syntax");
			System.out.println("SimpleProducer.jar <Topic> <Bootstrap Server>");
			return;
		}

		TOPIC = args[0];
		BOOTSTRAP_SERVER = args[1];

		System.out.println("BOOTSTRAP_SERVER: " + BOOTSTRAP_SERVER);
		System.out.println("TOPIC: " + TOPIC);

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", BOOTSTRAP_SERVER + ":9092");

		props.put("client.id", "client-id-" + new Date().getTime());
		
		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {
			String msg = new Date().toString();
			System.out.println(msg);
			producer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), msg), new AckCallBack());
		}
		
		producer.close();
	}

}

class AckCallBack implements Callback {

	@Override
	public void onCompletion(RecordMetadata arg0, Exception arg1) {
		if(arg0 != null){
			System.out.println("Offset: " + arg0.offset());
		}
		if (arg1 != null){
			arg1.printStackTrace();
		}else{
			System.out.println("Message sent successfully");
		}
	}

}