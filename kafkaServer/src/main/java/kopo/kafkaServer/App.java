package kopo.kafkaServer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
	// 토픽 이름
	private static final String TOPIC_NAME = "quickstart-events";

	// 종료할 메시지
	private static final String FIN_MESSAGE = "exit";

	private final static Logger log = LoggerFactory.getLogger("App");

	public static void main(String[] args) {
		// 카프카 환경 설정 객체
		Properties properties = new Properties();

		// 접속할 카프카 서버 정보
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_server:9092");

		// 전달할 데이터 구조는 Map구조인 키와 값 형태의 데이터 구조로 메시지를 보냄
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// 전달할 데이터 구조느 Map구조인 키의 데이터타입 : 문자열
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// 사용할 토픽명
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_NAME);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Collections.singletonList(TOPIC_NAME));

		String message = "";

		try {
			do {
				// 전달받는 대기시간은 최대 100.100ms = 100초
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000));

				for (ConsumerRecord<String, String> record : records) {
					message = record.value();

					// 받은 메시지를 No-SQL DB에 저장하는 로직 사용
					// 데이터 저장만 함
					log.info("My Kafka Server Message : " + message);
				}
			} while (message.equals(FIN_MESSAGE)); // exit 메시지지 전달될 때까지 동작

		} catch (Exception e) {
			log.info("Error : " + e.toString());
		} finally {
			consumer.close();
		}
	}
}
