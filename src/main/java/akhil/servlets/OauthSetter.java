package akhil.servlets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.util.OauthCredentials;
import akhil.util.OauthIDSecret;
import akhil.util.StringOps;

public class OauthSetter extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> id = StringOps.getInBetweenFast(reqBody, "<dmsv-client-id>", "</dmsv-client-id>", true, false);
		List<String> type = StringOps.getInBetweenFast(reqBody, "<dmsv-oauth-name>", "</dmsv-oauth-name>", true, false);
		List<String> secret = StringOps.getInBetweenFast(reqBody, "<dmsv-client-secret>", "</dmsv-client-secret>",
				true, false);
		List<String> scope = StringOps.getInBetweenFast(reqBody, "<dmsv-scope>", "</dmsv-scope>", true, false);
		List<String> grantType = StringOps.getInBetweenFast(reqBody, "<dmsv-grant-type>", "</dmsv-grant-type>", true, false);

		if (!id.isEmpty() && !secret.isEmpty() && !type.isEmpty()) {

			OauthIDSecret o = new OauthIDSecret();
			if (scope.isEmpty() && grantType.isEmpty())
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), "null", "null", false);
			else
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), scope.get(0).trim(),
						grantType.get(0).trim(), false);
			OauthCredentials.getInstance().addCreds(type.get(0).trim(), o);

			response.setStatus(201);
			response.getWriter().append("<dmsv-status>SUCCESS: Information updated.</dmsv-status>");

		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>ERROR: Required information not found. Fields <dmsv-oauth-name>, <dmsv-client-id> and <dmsv-client-secret> are mandatory.</dmsv-status>");
		}

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name = request.getParameter("dmsv-oauth-name").trim();
		OauthCredentials oc = OauthCredentials.getInstance();
		if (name.equals("FETCH_ALL_CREDS")) {
			Map<String, OauthIDSecret> map = oc.getCreds();
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String, OauthIDSecret> entry : map.entrySet()) {
				if (entry != null) {
					sb.append("<dmsv-creds-name>");
					sb.append(entry.getKey());
					sb.append("</dmsv-creds-name>");
					sb.append("<dmsv-client-id>");
					sb.append(entry.getValue().getOauthClientID());
					sb.append("</dmsv-client-id>\n<dmsv-client-secret>");
					sb.append(entry.getValue().getOauthClientSecret());
					sb.append("</dmsv-client-secret>\n<dmsv-scope>");
					sb.append(entry.getValue().getOauthScope());
					sb.append("</dmsv-scope>\n<dmsv-grant-type>");
					sb.append(entry.getValue().getOauthGrantType());
					sb.append("</dmsv-grant-type>\n");

				}
				response.setStatus(200);
				response.getWriter().append("<dmsv-status>\n" + sb.toString() + "</dmsv-status>");
			}
		} else {
			OauthIDSecret o = oc.getCreds(name);
			if (o != null) {
				String resp = "<dmsv-client-id>" + o.getOauthClientID() + "</dmsv-client-id>\n<dmsv-client-secret>"
						+ o.getOauthClientSecret() + "</dmsv-client-secret>\n<dmsv-scope>" + o.getOauthScope()
						+ "</dmsv-scope>\n<dmsv-grant-type>" + o.getOauthGrantType() + "</dmsv-grant-type>\n";
				response.setStatus(200);
				response.getWriter().append("<dmsv-status>\n" + resp + "</dmsv-status>");
			} else {
				response.setStatus(404);
				response.getWriter().append("<dmsv-status>ERROR: NOT FOUND</dmsv-status>");
			}
		}
	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name = request.getParameter("dmsv-oauth-name");
		String resp = OauthCredentials.getInstance().removeCreds(name);
		response.setStatus(200);
		response.getWriter().append("<dmsv-status>\n" + resp + "</dmsv-status>");

	}

}
