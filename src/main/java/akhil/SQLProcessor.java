package akhil;

import java.util.List;

import akhil.util.SQLite;
import akhil.util.StringOps;
import java.util.logging.Logger;

public class SQLProcessor {

	private static String dbUrl;
	private static final Logger LOGGER = Logger.getLogger(SQLProcessor.class.getName());

	public static void initialize(String dbUrlWindows, String dbUrlUnix) {
		if (System.getProperty("os.name").toLowerCase().contains("win"))
			dbUrl = dbUrlWindows;
		else
			dbUrl = dbUrlUnix;
	}

	public static String getDBUrl()
	{
		return dbUrl;
	}
	public static String deleteSQL(String name) {
		String queryDelete = "delete from datadashboardsqls where name = ?";
		Integer i = SQLite.getInstance(dbUrl).dmlQuery(queryDelete, name);
		if (i != null) {
			return "SUCCESS: Deleted " + i + " rows...";
		} else {
			return "ERROR: in delete process... check logs";
		}
	}

	public static String getSQL(String name) {

		if (name.equals("FETCH_ALL_SQLS")) {
			String querySelect = "select json_group_array( json_object( 'name', name, 'query' , query ) ) from datadashboardsqls";
			List<List<String>> result = SQLite.getInstance(dbUrl).selectQuery(querySelect, false);

			if (result.isEmpty()) {
				return "{ \"result\": [] }";
			} else {
				return "{ \"result\": \"" + result.get(0).get(0).replace("\"", "\\\"") + "\" }";
			}

		} else {

			String querySelect = "select query from datadashboardsqls where name = ?";
			List<List<String>> result = SQLite.getInstance(dbUrl).selectQuery(querySelect, false, name);

			if (result.isEmpty()) {
				return "{ \"result\": [] }";
			} else {
				return "{ \"result\": \"" + result.get(0).get(0).replace("\"", "\\\"") + "\" }";
			}
		}
	}

	public static String updateSQL(String name, String query) {
		String queryInsert = "update datadashboardsqls set query = ? where name = ?";
		Integer i = SQLite.getInstance(dbUrl).dmlQuery(queryInsert, query, name);
		if (i != null) {
			return "SUCCESS: Updated " + i + " rows...";
		} else {
			return "ERROR: Failed update... check logs";
		}
	}

	public static String saveSQL(String name, String query) {
		String queryInsert = "insert into datadashboardsqls (name, query) values (?, ?)";
		Integer i = SQLite.getInstance(dbUrl).dmlQuery(queryInsert, name, query);
		if (i != null) {
			return "SUCCESS: Inserted " + i + " rows...";
		} else {
			return "ERROR: Failed insert... check logs";
		}
	}

	public static String runSQLJsonOutput(String name, List<String> fr, String[] filters) {
		String querySelect = "select query from datadashboardsqls where name = ?";
		List<List<String>> result = SQLite.getInstance(dbUrl).selectQuery(querySelect, false, name);

		if (result.isEmpty()) {
			return "ERROR: select query not found";
		} else {
			String query = result.get(0).get(0);
			if (!fr.isEmpty())
			{
				for (String s : fr)
				{
					String find = StringOps.getInBetweenFast(s, "<findString>", "</findString>", true, false).get(0);
					String replace = StringOps.getInBetweenFast(s, "<replaceString>", "</replaceString>", true, false).get(0);
					query = query.replace(find, replace);
				}
			}
			LOGGER.fine("debug...." + query);
			if (filters == null)
				result = SQLite.getInstance(dbUrl).selectQuery(query, false);
			else {
				if (filters.length > 0)
					result = SQLite.getInstance(dbUrl).selectQuery(query, false, filters);
				else
					result = SQLite.getInstance(dbUrl).selectQuery(query, false);
			}
			if (result.isEmpty())
				return "ERROR: select query execution has errors... check logs";
			else
				return "{ \"result\":" + result.get(0).get(0) + " }";
		}

	}
	
	public static String runSQLDml(String name, List<String> fr, String[] filters) {
		String querySelect = "select query from datadashboardsqls where name = ?";
		List<List<String>> result = SQLite.getInstance(dbUrl).selectQuery(querySelect, false, name);

		if (result.isEmpty()) {
			return "ERROR: select query not found";
		} else {
			String query = result.get(0).get(0);
			if (fr!=null && !fr.isEmpty())
			{
				for (String s : fr)
				{
					String find = StringOps.getInBetweenFast(s, "<findString>", "</findString>", true, false).get(0);
					String replace = StringOps.getInBetweenFast(s, "<replaceString>", "</replaceString>", true, false).get(0);
					query = query.replace(find, replace);
				}
			}
			LOGGER.fine("debug...." + query);
			Integer res;
			if (filters == null)
				res = SQLite.getInstance(dbUrl).dmlQuery(query, new String[0]);
			else {
				if (filters.length > 0)
					res = SQLite.getInstance(dbUrl).dmlQuery(query, filters);
				else
					res = SQLite.getInstance(dbUrl).dmlQuery(query, new String[0]);
			}
			
			return "SUCCESS: Records updated/inserted: "+res;
		}

	}

}
