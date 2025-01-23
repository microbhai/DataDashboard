package akhil.servlets;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.DataUnlimited.DataUnlimitedApi;
import akhil.DataUnlimited.model.types.Types;
import akhil.DataUnlimited.util.FileOperation;
import akhil.util.StringOps;
import akhil.ApiCallProcessor;
import akhil.KafkaCallProcessor;
import akhil.TemplateProcessor;

public class DataGenerator extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = Logger.getLogger(DataGenerator.class.getName());

	public DataGenerator() {
		super();
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String body = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		String dir = StringOps.getInBetweenFast(body, "<dmsv-custom-datatype-param-dir>",
				"</dmsv-custom-datatype-param-dir>", true, false).get(0);

		if (!Types.checkCustomParamDirExists(dir)) {
			String msg = "";
			try {
				new DataUnlimitedApi(dir);
			} catch (Exception e) {
				msg = e.getMessage();
			} finally {
				if (!Types.checkCustomParamDirExists(dir)) {
					response.setStatus(500);
					response.getWriter()
							.append("<dmsv-status>ERROR: " + dir
									+ " couldn't be added. It is possible that it couldn't be found on the server. "
									+ msg + "</dmsv-status>");
				} else {
					response.setStatus(201);
					response.getWriter().append(
							"<dmsv-status>SUCCESS: " + dir + " added successfully as custom param dir</dmsv-status>");
				}
			}
		} else {
			response.setStatus(202);
			response.getWriter()
					.append("<dmsv-status>SUCCESS: " + dir + " already added to custom param dir list</dmsv-status>");
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		Enumeration<String> headers = request.getHeaders("dmsv-substitution-argument");
		String numOfFiles = request.getParameter("dmsv-num-of-files");
		String toLog = request.getParameter("dmsv-include-logs");
		String tname = request.getParameter("dmsv-template");

		if (tname != null) {
			String template = TemplateProcessor.getTemplate(tname);
			if (template.contains("ERROR")) {
				response.setContentType("text/txt");
				response.setStatus(400);
				response.getWriter().append("ERROR: Template not found");

			} else {

				String dmsScript = template;
				while (headers.hasMoreElements()) {
					String value = headers.nextElement();
					if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
						List<String> findReplace = StringOps.fastSplit(value, "###");
						String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
						String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
						dmsScript = dmsScript.replace(find, replace);
					}
				}

				if (numOfFiles == null)
					numOfFiles = "1";

				List<String> result = new DataUnlimitedApi().generateData(dmsScript, numOfFiles);

				if (toLog != null && toLog.equalsIgnoreCase("true")) {
					response.setContentType("text/txt");
					response.setStatus(201);
					response.getWriter().append(result.get(0) + result.get(1));
				} else {
					response.setContentType("text/txt");
					response.setStatus(201);
					response.getWriter().append(result.get(0));
				}
			}
		} else {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append("ERROR: No template name provided");
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		String body = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		List<String> payload = StringOps.getInBetweenFast(body, "<dmsv-payload>", "</dmsv-payload>", true, true);
		if (payload.isEmpty()) {

			Enumeration<String> headers = request.getHeaders("dmsv-substitution-argument");
			Enumeration<String> headerNumOfFiles = request.getHeaders("dmsv-num-of-files");
			Enumeration<String> headerIncludeLogs = request.getHeaders("dmsv-include-logs");
			String numOfFiles = "1";
			String toLog = "false";

			while (headerNumOfFiles.hasMoreElements()) {
				numOfFiles = headerNumOfFiles.nextElement();
			}
			while (headerIncludeLogs.hasMoreElements()) {
				toLog = headerIncludeLogs.nextElement();
			}

			List<String> tname = StringOps.getInBetweenFast(body, "<dmsv-template>", "</dmsv-template>", true, true);

			if (!tname.isEmpty()) {
				String postExternal = null;
				String postexternaltype = null;
				List<String> postexternalL = StringOps.getInBetweenFast(body, "<dmsv-post-external>",
						"</dmsv-post-external>", true, true);

				if (!postexternalL.isEmpty()) {
					postExternal = postexternalL.get(0);

					List<String> postexternaltypeL = StringOps.getInBetweenFast(body, "<dmsv-post-external-type>",
							"</dmsv-post-external-type>", true, true);
					if (!postexternaltypeL.isEmpty())
						postexternaltype = postexternaltypeL.get(0);
					else
						postexternaltype = "API";
				}
				LOGGER.info("INFO: prepared statement value set." + postExternal);
				String dmsScriptAddendum = StringOps
						.getInBetweenFast(body, "<dmsv-script-addendum>", "</dmsv-script-addendum>", true, false)
						.get(0);

				String template = TemplateProcessor.getTemplate(tname.get(0));
				if (template.contains("ERROR")) {
					response.setContentType("text/txt");
					response.setStatus(400);
					response.getWriter().append("ERROR: Template not found");

				} else {
					String dmsScript;

					if (dmsScriptAddendum != null)
						dmsScript = template + dmsScriptAddendum;
					else
						dmsScript = template;

					while (headers.hasMoreElements()) {
						String value = headers.nextElement();
						if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
							List<String> findReplace = StringOps.fastSplit(value, "###");
							String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
							String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
							dmsScript = dmsScript.replace(find, replace);
						}
					}

					if (Integer.parseInt(numOfFiles) > 10) {

						String newFolder = UUID.randomUUID().toString();
						String filepath = ApiCallProcessor.getLogDir() + File.separator + newFolder;
						new DataUnlimitedApi().generateData(dmsScript, numOfFiles, filepath, "txt");

						response.setContentType("text/txt");
						response.setStatus(201);

						if (postExternal != null) {
							if (postexternaltype.equalsIgnoreCase("API")) {
								String resp = ApiCallProcessor.process(postExternal, filepath, true);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append(resp);
								response.getWriter().append("\n</postexternal-result>");
							} else if (postexternaltype.equalsIgnoreCase("KAFKA")) {
								String resp = KafkaCallProcessor.process(postExternal, filepath, true);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append(resp);
								response.getWriter().append("\n</postexternal-result>");
							} else {
								response.setStatus(501);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append("Post type specified is not supported yet.");
								response.getWriter().append("\n</postexternal-result>");
							}
						} else {
							response.setStatus(400);
							response.getWriter().append("\n<postexternal-result>");
							response.getWriter().append(
									"Only upto 10 data files can be displayed in UI. Use more than 10 files for posting externally to API/Kafka");
							response.getWriter().append("\n</postexternal-result>");

						}

						FileOperation.deleteFile(filepath, "txt");

					} else {
						List<String> result = new DataUnlimitedApi().generateData(dmsScript, numOfFiles);

						response.setContentType("text/txt");
						response.setStatus(201);

						if (toLog != null && toLog.equalsIgnoreCase("true"))
							response.getWriter().append(result.get(0) + result.get(1));
						else
							response.getWriter().append(result.get(0));

						if (postExternal != null) {
							if (postexternaltype.equalsIgnoreCase("API")) {
								String resp = ApiCallProcessor.process(postExternal, result.get(0), false);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append(resp);
								response.getWriter().append("\n</postexternal-result>");
							} else if (postexternaltype.equalsIgnoreCase("KAFKA")) {
								String resp = KafkaCallProcessor.process(postExternal, result.get(0), false);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append(resp);
								response.getWriter().append("\n</postexternal-result>");
							} else {
								response.setStatus(501);
								response.getWriter().append("\n<postexternal-result>");
								response.getWriter().append("Post type specified is not supported yet.");
								response.getWriter().append("\n</postexternal-result>");
							}
						}
					}
				}
			}

			else {

				String dmsScript = body;
				while (headers.hasMoreElements()) {
					String value = headers.nextElement();
					if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
						List<String> findReplace = StringOps.fastSplit(value, "###");
						String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
						String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
						dmsScript = dmsScript.replace(find, replace);
					}
				}
				List<String> result = new DataUnlimitedApi().generateData(dmsScript, numOfFiles);

				response.setContentType("text/txt");
				response.setStatus(201);

				if (toLog != null && toLog.equalsIgnoreCase("true"))
					response.getWriter().append(result.get(0) + result.get(1));
				else
					response.getWriter().append(result.get(0));
			}
		}
		else
		{
			String postExternal = null;
			String postexternaltype = null;
			List<String> postexternalL = StringOps.getInBetweenFast(body, "<dmsv-post-external>",
					"</dmsv-post-external>", true, true);

			if (!postexternalL.isEmpty()) {
				postExternal = postexternalL.get(0);

				List<String> postexternaltypeL = StringOps.getInBetweenFast(body, "<dmsv-post-external-type>",
						"</dmsv-post-external-type>", true, true);
				if (!postexternaltypeL.isEmpty())
					postexternaltype = postexternaltypeL.get(0);
				else
					postexternaltype = "API";
				
				if (postexternaltype.equalsIgnoreCase("API")) {
					String resp = ApiCallProcessor.process(postExternal, payload.get(0), false);
					response.getWriter().append("\n<postexternal-result>");
					response.getWriter().append(resp);
					response.getWriter().append("\n</postexternal-result>");
				} else if (postexternaltype.equalsIgnoreCase("KAFKA")) {
					String resp = KafkaCallProcessor.process(postExternal, payload.get(0), false);
					response.getWriter().append("\n<postexternal-result>");
					response.getWriter().append(resp);
					response.getWriter().append("\n</postexternal-result>");
				} else {
					response.setStatus(501);
					response.getWriter().append("\n<postexternal-result>");
					response.getWriter().append("Post type specified is not supported yet.");
					response.getWriter().append("\n</postexternal-result>");
				}
			}
			else
			{
				response.setContentType("text/txt");
				response.setStatus(400);
				response.getWriter().append("ERROR: <dmsv-post-external> is mandatory for payload posting");
			}

			
			
		}
	}

}
