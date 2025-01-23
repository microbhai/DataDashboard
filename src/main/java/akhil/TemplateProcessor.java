package akhil;

import java.util.List;
import akhil.util.SQLite;

public class TemplateProcessor {

	public static int deleteTemplate(String tname) {
		String query = "delete from TEMPLATE_STORE where NAME = ?";
		int result = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, tname);
		return result;
	}

	public static Integer updateTemplate(String tname, String tdes, String script) {
		String query = "update TEMPLATE_STORE set DESCRIPTION = ?, DMS_SCRIPT_CONTENT = ? where NAME = ?";
		Integer result = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, tdes, script, tname);
		return result;
	}

	public static String getTemplate(String tname) {
		String query = "select DMS_SCRIPT_CONTENT from TEMPLATE_STORE where NAME = ? order by NAME";
		List<List<String>> qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
		if (!qr.isEmpty() && !qr.get(0).isEmpty())
			return qr.get(0).get(0);
		else
			return "ERROR: NOT FOUND";
	}

	public static String getTemplateSearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME like ? order by NAME";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME like ? or DESCRIPTION like ? order by NAME";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%",
						"%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where DESCRIPTION like ? order by NAME";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select NAME, DESCRIPTION, DMS_SCRIPT_CONTENT from TEMPLATE_STORE where NAME = ?";
				qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, DESCRIPTION, DMS_SCRIPT_CONTENT from TEMPLATE_STORE order by NAME";
			qr = SQLite.getInstance(SQLProcessor.getDBUrl()).selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-template-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-template-search-result>");
			sb.append("<dmsv-template-search-name>" + ls.get(0) + "</dmsv-template-search-name>");
			sb.append("<dmsv-template-search-description>" + ls.get(1) + "</dmsv-template-search-description>");
			if (ls.size() == 3) {
				sb.append("<dmsv-template-search-dmsvscript>");
				sb.append(ls.get(2));
				sb.append("</dmsv-template-search-dmsvscript>");
			}
			sb.append("</dmsv-template-search-result>");

		}
		sb.append("</dmsv-template-search-results>");

		return sb.toString();

	}

	public static Integer saveTemplate(String tname, String tdes, String script) {
		String query = "insert into TEMPLATE_STORE(NAME, DESCRIPTION, DMS_SCRIPT_CONTENT) values (?,?,?)";
		Integer result = SQLite.getInstance(SQLProcessor.getDBUrl()).dmlQuery(query, tname, tdes, script);
		return result;
	}
}
