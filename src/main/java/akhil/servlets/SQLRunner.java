package akhil.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.logging.Logger;

import akhil.SQLProcessor;
import akhil.util.StringOps;

public class SQLRunner extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = Logger.getLogger(SQLRunner.class.getName());

	public SQLRunner() {
		super();
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> names = StringOps.getInBetweenFast(reqBody, "<name>", "</name>", true, false);
		List<String> filters = StringOps.getInBetweenFast(reqBody, "<filter>", "</filter>", true, false);
		List<String> formats = StringOps.getInBetweenFast(reqBody, "<format>", "</format>", true, false);
		List<String> fr = StringOps.getInBetweenFast(reqBody, "<find-replace>", "</find-replace>", true, false);

		String result = null;

		if (!names.isEmpty() && !formats.isEmpty()) {
			String name = names.get(0).trim();
			String format = formats.get(0).trim();
			List<String> findreplace = null;
			if (format.equals("json")) {

				if (!fr.isEmpty()) {
					findreplace = fr;
				}

				if (!filters.isEmpty()) {
					List<String> filter = StringOps.fastSplit(filters.get(0).trim(),"<delim>");
					result = SQLProcessor.runSQLJsonOutput(name, findreplace, filter.toArray(new String[0]));
				} else
					result = SQLProcessor.runSQLJsonOutput(name, findreplace, null);
			} else if (format.equals("dml")) {
				if (!fr.isEmpty()) {
					findreplace = fr;
				}

				if (!filters.isEmpty()) {
					String[] filter = filters.get(0).trim().split("<delim>");
					result = SQLProcessor.runSQLDml(name, findreplace, filter);
				}
				else result = SQLProcessor.runSQLDml(name, findreplace, null);
			} else {

				response.setStatus(400);
				response.getWriter().append(
						"<status>ERROR: Only json and dml (insert/update queries) formats are supported for now.</status>");

			}

			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append("<status>ERROR:" + result + "</status>");
			} else {
				response.setStatus(200);
				LOGGER.fine("debug...." + result);
				response.getWriter().append(result);
			}

		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<status>ERROR: Required fields missing in the request. <name></name>, <format></format> is mandatory.</status>");
		}

	}

}
