package akhil.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.util.StringOps;
import akhil.KafkaCallProcessor;

public class KafkaCallRegister extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public KafkaCallRegister() {
		super();
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name_ = request.getParameter("names");
		List<String> names = StringOps.fastSplit(name_, ",");

		if (!names.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String name : names) {

				sb.append(KafkaCallProcessor.deleteKafkaCall(name));

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

		String searchtype = request.getParameter("search-type");
		String tname = request.getParameter("dmsv-kafka");

		String searchResults = KafkaCallProcessor.getKafkaSearchResults(tname, searchtype);

		response.setContentType("text/txt");
		response.setStatus(200);
		response.getWriter().append(searchResults);

	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> calls = StringOps.getInBetweenFast(reqBody, "<kafkacall>", "</kafkacall>", true, false);

		if (!calls.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String call : calls) {
				List<String> name = StringOps.getInBetweenFast(call, "<name>", "</name>", true, false);
				List<String> details = StringOps.getInBetweenFast(call, "<details>", "</details>", true, false);
				List<String> descriptions = StringOps.getInBetweenFast(call, "<description>", "</description>", true, true);

				if (name.isEmpty() || details.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>ERROR: Required fields for Kafka Call are missing in the request. <name></name> and <details></details> are mandatory.</status>");
				} else {

					String name_ = name.get(0).trim();
					String details_ = "";
					if (!details.isEmpty())
						details_ = details.get(0).trim();
					String descriptions_ = "";

					if (!descriptions.isEmpty())
						descriptions_ = descriptions.get(0).trim();

					String result = KafkaCallProcessor.updateKafkaCall(name_, details_, descriptions_);
					sb.append(result + '\n');

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
					"<status>ERROR: Required fields missing in the request. <kafkacall></kafkacall> is mandatory.</status>");
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> calls = StringOps.getInBetweenFast(reqBody, "<kafkacall>", "</kafkacall>", true, false);

		if (!calls.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String call : calls) {
				List<String> name = StringOps.getInBetweenFast(call, "<name>", "</name>", true, true);
				List<String> details = StringOps.getInBetweenFast(call, "<details>", "</details>", true, true);
				List<String> descriptions = StringOps.getInBetweenFast(call, "<description>", "</description>", true, true);

				if (name.isEmpty() || details.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>ERROR: Required fields for Kafka Call are missing in the request. <name></name> and <details></details> are mandatory.</status>");
				} else {

					String name_ = name.get(0).trim();
					String details_ = details.get(0).trim();
					String descriptions_ = "";

					if (!descriptions.isEmpty())
						descriptions_ = descriptions.get(0).trim();

					String result = KafkaCallProcessor.saveKafkaCall(name_, details_, descriptions_);
					sb.append(result + '\n');

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
					"<status>ERROR: Required fields missing in the request. <kafkacall></kafkacall> is mandatory.</status>");
		}
	}
}
