package akhil.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.util.StringOps;
import akhil.SQLProcessor;

public class SQLRegister extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public SQLRegister() {
		super();
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> names = StringOps.getInBetweenFast(reqBody, "<name>", "</name>", true, false);

		if (!names.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String name : names) {

				sb.append(SQLProcessor.deleteSQL(name));

			}

			String result = sb.toString();
			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append("<status>ERROR:" + result + "</status>");
			} else {
				response.setStatus(200);
				response.getWriter().append("<status>SUCCESS</status>");
			}
		} else {

			response.setStatus(400);
			response.getWriter().append(
					"<status>ERROR: Required fields missing in the request. <name></name> is mandatory.</status>");
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> names = StringOps.getInBetweenFast(reqBody, "<name>", "</name>", true, false);

		if (!names.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String name : names) {

				sb.append(SQLProcessor.getSQL(name));

			}

			String result = sb.toString();
			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append("<status>ERROR:" + result + "</status>");
			} else {
				response.setStatus(200);
				response.getWriter().append(result);
			}
		} else {

			response.setStatus(400);
			response.getWriter().append(
					"<status>ERROR: Required fields missing in the request. <name></name> is mandatory.</status>");
		}
		
		
	}
	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> sqls = StringOps.getInBetweenFast(reqBody, "<sql>", "</sql>", true, false);

		if (!sqls.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String sql : sqls) {
				List<String> name = StringOps.getInBetweenFast(sql, "<name>", "</name>", true, false);
				List<String> query = StringOps.getInBetweenFast(sql, "<query>", "</query>", true, false);

				if (name.isEmpty() || query.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>ERROR: Required fields for SQL arew missing in the request. <name></name> and <query></query> are mandatory.</status>");
				} else {

					String name_ = name.get(0).trim();
					String query_ = query.get(0).trim();

					String result = SQLProcessor.updateSQL(name_, query_);
					sb.append(result+'\n');
					
				}
			}
			String result = sb.toString();
			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append("<status>ERROR: " + result + "</status>");
			} else {
				response.setStatus(200);
				response.getWriter().append("<status>SUCCESS</status>");
			}
			
			
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<status>ERROR: Required fields missing in the request. <sql></sql> is mandatory.</status>");
		}
	}
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> sqls = StringOps.getInBetweenFast(reqBody, "<sql>", "</sql>", true, false);

		if (!sqls.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String sql : sqls) {
				List<String> name = StringOps.getInBetweenFast(sql, "<name>", "</name>", true, false);
				List<String> query = StringOps.getInBetweenFast(sql, "<query>", "</query>", true, false);

				if (name.isEmpty() || query.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>ERROR: Required fields for SQL arew missing in the request. <name></name> and <query></query> are mandatory.</status>");
				} else {

					String name_ = name.get(0).trim();
					String query_ = query.get(0).trim();

					String result = SQLProcessor.saveSQL(name_, query_);
					sb.append(result+'\n');
					
				}
			}
			String result = sb.toString();
			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append("<status>ERROR: " + result + "</status>");
			} else {
				response.setStatus(200);
				response.getWriter().append("<status>SUCCESS</status>");
			}
			
			
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<status>ERROR: Required fields missing in the request. <sql></sql> is mandatory.</status>");
		}
	}
}
