package samples;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class ClusterRunner {

	public static class Generator implements Runnable {
		public void run() {
			try {
				ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
				Connection connection = factory.createConnection();
				connection.start();

				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				Destination destination = session.createQueue("income");
				MessageProducer producer = session.createProducer(destination);

				for (int i = 1; i <= 10; i++) {
					TextMessage message = session.createTextMessage("message #" + i);
					producer.send(message);
				}
			} catch (Exception e) {
				System.err.println(e);
			}
		}
	}

	public static class Node implements Runnable {

		private long startTime;
		private String nodeName;

		public Node(long startTime, String nodeName) {
			this.startTime = startTime;
			this.nodeName = nodeName;
		}

		public void run() {
			try {
				ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
				Connection connection = factory.createConnection();
				connection.start();

				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				Destination destination = session.createQueue("income");
				MessageConsumer consumer = session.createConsumer(destination);

				while (true) {
					TextMessage message = (TextMessage) consumer.receive();
					Thread.sleep(1000);
					long time = System.currentTimeMillis() - startTime;
					System.out.println(message.getText() + ", processed by " + nodeName + ", time = " + time);
				}
			} catch (Exception e) {
				System.err.println(e);
			}
		}
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();

		new Thread(new Generator()).start();

		new Thread(new Node(startTime, "node #1")).start();
		new Thread(new Node(startTime, "node #2")).start();
		new Thread(new Node(startTime, "node #3")).start();
		new Thread(new Node(startTime, "node #4")).start();
		new Thread(new Node(startTime, "node #5")).start();
	}

}
