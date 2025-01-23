package akhil;

import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import akhil.util.StringOps;
import akhil.DataUnlimited.util.FileOperation;
import akhil.util.KafkaMessageProducer;
import akhil.util.OauthIDSecret;
import akhil.util.OauthCredentials;
import akhil.util.SQLite;

public class KafkaCallProcessor {

	private static final Logger LOGGER = Logger.getLogger(KafkaCallProcessor.class.getName());

	public static void initialize() {
		String query = "select ID, SECRET, TYPE, OAUTHSCOPE, GRANTTYPE from VS_OKTA_CREDENTIALS";

		List<List<String>> qr1 = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false);
		if (!qr1.isEmpty()) {
			for (List<String> ls : qr1) {
				OauthIDSecret o = new OauthIDSecret();
				o.setOauth(ls.get(0), ls.get(1), ls.get(2), ls.get(3), ls.get(4), true);
				OauthCredentials.getInstance().addCreds(ls.get(2), o);
			}
		}
	}

	public static String process(String kafkaName, String data, boolean isDataFolderPath) {

		String querySelect = "select details from kafkacalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect, false,
				kafkaName);
		if (result.isEmpty())
			return "ERROR: No Kafka Call Details found for name: " + kafkaName;
		else {
			String kafkaDetails = result.get(0).get(0);
			List<String> url = StringOps.getInBetweenFast(kafkaDetails, "<broker-url>", "</broker-url>", true, true);
			List<String> oauthtokenurl = StringOps.getInBetweenFast(kafkaDetails, "<oauth-token-url>",
					"</oauth-token-url>", true, true);
			List<String> compressionL = StringOps.getInBetweenFast(kafkaDetails, "<compression>", "</compression>",
					true, true);
			String compression = "none";
			if (!compressionL.isEmpty())
				compression = compressionL.get(0);
			List<String> topicName = StringOps.getInBetweenFast(kafkaDetails, "<topic>", "</topic>", true, true);
			List<String> oauthcredsname = StringOps.getInBetweenFast(kafkaDetails, "<oauth-creds-name>",
					"</oauth-creds-name>", true, true);

			if (url.isEmpty())
				return "ERROR: Broker URL missing";
			else if (oauthtokenurl.isEmpty())
				return "ERROR: Oauth-token-url missing";
			else if (topicName.isEmpty())
				return "ERROR: Topic name missing";
			else if (oauthcredsname.isEmpty())
				return "ERROR: oauth-creds-name name missing";
			else {

				OauthIDSecret os = OauthCredentials.getInstance().getCreds(oauthcredsname.get(0));

				if (os != null) {

					String id = os.getOauthClientID();
					String secret = os.getOauthClientSecret();

					KafkaMessageProducer producer = new KafkaMessageProducer(url.get(0), oauthtokenurl.get(0), id, secret,
							compression);
					String toReturn;

					if (isDataFolderPath) {
						List<String> files = FileOperation.getListofFiles(data, true, false);
						for (String file : files) {
							String payload = FileOperation.getFileContentAsString(file);
							ProducerRecord<String, String> record = new ProducerRecord<>(topicName.get(0), payload);
							RecordMetadata res = producer.send(record);

							StringBuilder sb = new StringBuilder();

							sb.append("\n------------------------------------------------------------\n");
							if (res != null)
								sb.append("Message posted to Kafka: partition,offset = " + res.partition() + ","
										+ res.offset());
							else
								sb.append("ERROR: Message post result to Kafka is null");
							sb.append("\n------------------------------------------------------------\n");
							FileOperation.writeFile(ApiCallProcessor.getLogDir(), "DataGenerator_PostAPI.log",
									sb.toString(), "utf8");
						}

						toReturn = "Post response data will be written to log files (for more than 10 files). Check directory: "
								+ ApiCallProcessor.getLogDir();

					} else {

						StringBuilder sb = new StringBuilder();
						if (data.contains("<data-file>") && data.contains("</data-file>")) {
							List<String> dataFiles = StringOps.getInBetweenFast(data, "<data-file>", "</data-file>",
									true, false);

							for (String file : dataFiles) {
								LOGGER.info("debug.... sending multiple files");
								ProducerRecord<String, String> record = new ProducerRecord<>(topicName.get(0), file);
								RecordMetadata res = producer.send(record);
								if (res != null)
									sb.append("Message posted to Kafka: partition,offset = " + res.partition() + ","
											+ res.offset());
								else
									sb.append("ERROR: Message post result to Kafka is null");
							}

						} else {
							LOGGER.info("debug.... sending one file");
							ProducerRecord<String, String> record = new ProducerRecord<>(topicName.get(0), data);
							RecordMetadata res = producer.send(record);
							if (res != null)
								sb.append("Message posted to Kafka: partition,offset = " + res.partition() + ","
										+ res.offset() + "\n");
							else
								sb.append("ERROR: Message post result to Kafka is null\n");
						}
						toReturn = sb.toString();
					}
					producer.close();
					return toReturn;
				} else
					return "ERROR: oauth creds not found";
			}
		}
	}

	public static String deleteKafkaCall(String name) {
		String queryDelete = "delete from kafkacalldetails where name = ?";
		Integer i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(queryDelete, name);
		if (i != null) {
			return "SUCCESS: Deleted " + i + " rows...";
		} else {
			return "ERROR: in delete process... check logs";
		}
	}
	public static String getKafkaSearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select name, description from kafkacalldetails where name = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select name, description from kafkacalldetails where name like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select name, description from kafkacalldetails where name like ? or DESCRIPTION like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%",
						"%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select name, description from kafkacalldetails where DESCRIPTION like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select name, description, details from kafkacalldetails where name = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, description, details from kafkacalldetails order by NAME";
			qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-kafka-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-kafka-search-result>");
			sb.append("<dmsv-kafka-search-name>" + ls.get(0) + "</dmsv-kafka-search-name>");
			sb.append("<dmsv-kafka-search-description>" + ls.get(1) + "</dmsv-kafka-search-description>");
			if (ls.size() == 3) {
				sb.append("<dmsv-kafka-search-details>");
				sb.append(ls.get(2));
				sb.append("</dmsv-kafka-search-details>");
			}
			sb.append("</dmsv-kafka-search-result>");

		}
		sb.append("</dmsv-kafka-search-results>");

		return sb.toString();

	}
/*
	public static String getKafkaCall(String name) {

		if (name.equals("FETCH_ALL_KAFKACALLS")) {
			String querySelect = "select json_agg(kafkacalldetails) from kafkacalldetails";
			List<List<String>> result = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect, false);

			if (result.isEmpty()) {
				return "{ \"result\": [] }";
			} else {
				return "{ \"result\": \"" + result.get(0).get(0).replace("\"", "\\\"") + "\" }";
			}

		} else {

			String querySelect = "select details from kafkacalldetails where name = ?";
			List<List<String>> result = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect, false,
					name);

			if (result.isEmpty()) {
				return "NOT FOUND";
			} else {
				return result.get(0).get(0);
			}
		}
	}
*/
	public static String updateKafkaCall(String name, String details, String description) {
		Integer i;
		if (description.isEmpty())
		{
			String query = "update kafkacalldetails set details = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, name);
		}
		else if (details.isEmpty())
		{
			String query = "update kafkacalldetails set description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, description, name);
		}
		else {
			String query = "update kafkacalldetails set details = ?, description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, description, name);
		}
		if (i != null) {
			return StringOps.append("SUCCESS: Updated ", String.valueOf(i), " rows...");
		} else {
			return "ERROR: Failed update... check logs";
		}
	}

	public static String saveKafkaCall(String name, String details, String description) {
		String queryInsert = "insert into kafkacalldetails (name, description, details) values (?, ?, ?)";
		Integer i = SQLite.getInstance().dmlQuery(queryInsert, name, description, details);
		if (i != null) {
			return StringOps.append("SUCCESS: Inserted ", String.valueOf(i), " rows...");
		} else {
			return "ERROR: Failed insert... check logs";
		}
	}
}
