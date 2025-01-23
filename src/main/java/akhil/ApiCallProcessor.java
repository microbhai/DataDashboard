package akhil;

import java.util.List;
//import java.util.logging.Logger;
//import java.util.logging.Level;
//import java.util.logging.Logger;

import akhil.util.StringOps;
import akhil.DataUnlimited.util.FileOperation;
import akhil.util.OauthIDSecret;
import akhil.util.APICall;
import akhil.util.OauthCredentials;
import akhil.util.SQLite;

public class ApiCallProcessor {

	// private final static Logger LOGGER =
	// Logger.getLogger(ApiCallProcessor.class.getName());

	private static String logDir;

	public static void setLogDir(String s) {
		logDir = s;
	}

	public static String getLogDir() {
		return logDir;
	}

	public static void initialize() {

		String query = "select ID, SECRET, TYPE, GRANTTYPE, OAUTHSCOPE from VS_OKTA_CREDENTIALS";

		List<List<String>> qr1 = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false);
		if (!qr1.isEmpty()) {
			for (List<String> ls : qr1) {
				OauthIDSecret o = new OauthIDSecret();
				o.setOauth(ls.get(0), ls.get(1), ls.get(2), ls.get(3), ls.get(4), true);
				OauthCredentials.getInstance().addCreds(ls.get(2), o);
			}
		}
	}

	public static String process(String apiName, String data, boolean isDataFolderPath) {

		String querySelect = "select details from apicalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect, false,
				apiName);
		if (result.isEmpty())
			return "ERROR: No API Call Details found for name: " + apiName;
		else {
			String apiDetails = result.get(0).get(0);
			String url = StringOps.getInBetweenFast(apiDetails, "<url>", "</url>", true, false).get(0);
			List<String> oktaurl = StringOps.getInBetweenFast(apiDetails, "<okta-url>", "</okta-url>", true, true);
			String contentType = StringOps
					.getInBetweenFast(apiDetails, "<content-type>", "</content-type>", true, false).get(0);
			String oktacredsname = StringOps
					.getInBetweenFast(apiDetails, "<okta-creds-name>", "</okta-creds-name>", true, false).get(0);
			String headers = StringOps.getInBetweenFast(apiDetails, "<headers>", "</headers>", false, false).get(0);
			List<String> xcsrfs = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-url>", "</x-csrf-token-url>",
					true, true);
			List<String> xcsrfsHeaders = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-headers>",
					"</x-csrf-token-headers>", true, true);
			String bearerToken = null;

			if (oktaurl != null && !oktaurl.isEmpty())
				bearerToken = "Bearer " + APICall.getBearerToken(oktaurl.get(0),
						OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientID(),
						OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientSecret(),
						OauthCredentials.getInstance().getCreds(oktacredsname).getOauthScope(),
						OauthCredentials.getInstance().getCreds(oktacredsname).getOauthGrantType());

			APICall apicall = new APICall();
			String xcsrfToken = null;
			if (xcsrfs != null && !xcsrfs.isEmpty())
				xcsrfToken = apicall.getXcsrfToken(xcsrfs.get(0), bearerToken, xcsrfsHeaders);

			// LOGGER.log(Level.INFO, "BearerToken Token call : {0}", new Object[] {
			// bearerToken });

			if (isDataFolderPath) {

				List<String> files = FileOperation.getListofFiles(data, true, false);

				for (String file : files) {
					StringBuilder sb = new StringBuilder();
					String payload = FileOperation.getFileContentAsString(file);
					sb.append("\n------------------------------------------------------------\n");
					sb.append(apicall.makeCallPost(url, payload, contentType, bearerToken, headers, xcsrfToken));
					sb.append("\n------------------------------------------------------------\n");
					FileOperation.writeFile(logDir, "DataGenerator_PostAPI.log", sb.toString(), "utf8");
				}
				return "Post response data will be written to log files (for more than 10 files). Check directory: "
						+ logDir;

			} else {
				if (data.contains("<data-file>") && data.contains("</data-file>")) {
					List<String> dataFiles = StringOps.getInBetweenFast(data, "<data-file>", "</data-file>", true,
							true);
					StringBuilder sb = new StringBuilder();
					for (String file : dataFiles) {
						sb.append(apicall.makeCallPost(url, file, contentType, bearerToken, headers, xcsrfToken));
					}
					return sb.toString();
				} else {
					return apicall.makeCallPost(url, data, contentType, bearerToken, headers, xcsrfToken);
				}
			}
		}
	}

	public static String deleteAPICall(String name) {
		String queryDelete = "delete from apicalldetails where name = ?";
		Integer i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(queryDelete, name);
		if (i != null) {
			return "SUCCESS: Deleted " + i + " rows...";
		} else {
			return "ERROR: in delete process... check logs";
		}
	}

	public static String getAPISearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select name, description from apicalldetails where name = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select name, description from apicalldetails where name like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select name, description from apicalldetails where name like ? or DESCRIPTION like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%",
						"%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select name, description from apicalldetails where DESCRIPTION like ? order by name";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select name, description, details from apicalldetails where name = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, description, details from apicalldetails order by NAME";
			qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-api-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-api-search-result>");
			sb.append("<dmsv-api-search-name>" + ls.get(0) + "</dmsv-api-search-name>");
			sb.append("<dmsv-api-search-description>" + ls.get(1) + "</dmsv-api-search-description>");
			if (ls.size() == 3) {
				sb.append("<dmsv-api-search-details>");
				sb.append(ls.get(2));
				sb.append("</dmsv-api-search-details>");
			}
			sb.append("</dmsv-api-search-result>");

		}
		sb.append("</dmsv-api-search-results>");

		return sb.toString();

	}

	/*
	 * public static String getAPICall(String name) {
	 * 
	 * if (name.equals("FETCH_ALL_APICALLS")) { String querySelect =
	 * "select json_agg(apicalldetails) from apicalldetails"; List<List<String>>
	 * result = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect,
	 * false);
	 * 
	 * if (result.isEmpty()) { return "{ \"result\": [] }"; } else { return
	 * "{ \"result\": \"" + result.get(0).get(0).replace("\"", "\\\"") + "\" }"; }
	 * 
	 * } else {
	 * 
	 * String querySelect = "select details from apicalldetails where name = ?";
	 * List<List<String>> result =
	 * SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(querySelect, false,
	 * name);
	 * 
	 * if (result.isEmpty()) { return "NOT FOUND"; } else { return
	 * result.get(0).get(0); } } }
	 */
public static String updateAPICall(String name, String details, String description) {
		
		Integer i;
		if (description.isEmpty())
		{
			String query = "update apicalldetails set details = ? where name = ?";
			i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, details, name);
		}
		else if (details.isEmpty())
		{
			String query = "update apicalldetails set description = ? where name = ?";
			i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, description, name);
		}
		else {
			String query = "update apicalldetails set details = ?, description = ? where name = ?";
			i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, details, description, name);
		}
		if (i != null) {
			return StringOps.append("SUCCESS: Updated ", String.valueOf(i), " rows...");
		} else {
			return "ERROR: Failed update... check logs";
		}
	}

	public static String saveAPICall(String name, String details, String description) {

		String queryInsert = "insert into apicalldetails (name, description, details) values (?, ?, ?)";
		Integer i = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(queryInsert, name, description, details);
		if (i != null) {
			return StringOps.append("SUCCESS: Inserted ", String.valueOf(i), " rows...");
		} else {
			return "ERROR: Failed insert... check logs";
		}
	}
}
