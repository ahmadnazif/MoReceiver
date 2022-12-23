package com.cookiss.moreceiver;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.validator.routines.EmailValidator;
import org.apache.log4j.Logger;

import com.cookiss.moreceiver.object.CreditDetails;
import com.cookiss.moreceiver.object.MoManagement;

/**
 * Servlet implementation class GenereicReceiver
 */
public class GenericReceiver extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(GenericReceiver.class);

//	private static String STATUS_MANAGED = "OK";
	private static String STATUS_MANAGEMENT_NOT_FOUND = "Campaign Not Found";
	private static String STATUS_KEYWORD_INVALID = "Invalid Keyword";
	private static String STATUS_DATE_RANGE_ERROR = "Date Range Issue";
	private static String STATUS_NO_CREDITS = "No Credits";

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public GenericReceiver() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			System.out.println("DbConn: " + getServletContext().getInitParameter("dbUrl"));
			long startTime = System.currentTimeMillis();

			ServletContext ctx = getServletContext();

			String requestId = UUID.randomUUID().toString();

			@SuppressWarnings("unchecked")
			Enumeration<String> parameterNames = request.getParameterNames();

			while (parameterNames.hasMoreElements()) {
				String parameterName = parameterNames.nextElement();

				logger.info(requestId + ", Received: " + parameterName + ": " + request.getParameter(parameterName));
			}

			String from = request.getParameter("from");
			String to = request.getParameter("to");
			String transId = request.getParameter("transid");
			String msg = request.getParameter("msg");
			msg = new String(msg.getBytes("ISO-8859-1"), "UTF-8");

			logger.info("Received " + transId + ", " + from + "," + to + "," + msg);

			msg = msg.trim();
			
			int firstSpaceIndex = msg.indexOf(" ");
			int firstUnderscoreIndex = msg.indexOf("_");
			
			logger.info("First space index: " + firstSpaceIndex);
			logger.info("First underscore index: " + firstUnderscoreIndex);
			
			String[] inputArr = msg.split(" ");
			String keyword = inputArr[0];
			
			if (firstUnderscoreIndex != -1) {
				if (firstUnderscoreIndex < firstSpaceIndex) {
					logger.info("Split by underscore instead.");
					keyword = msg.split("_")[0];
				} else if (firstSpaceIndex == -1) {
					logger.info("Split by underscore instead.");
					keyword = msg.split("_")[0];
				}
			}

			boolean inserted = false;

			if (keyword == null || keyword.equals("")) {
				inserted = insertMo(ctx, transId, keyword, msg, STATUS_KEYWORD_INVALID, from, to, "", 0, 0);
			} else {
				MoManagement mgmt = getManagementDetails(ctx, keyword);

				if (mgmt == null) {
					inserted = insertMo(ctx, transId, keyword, msg, STATUS_MANAGEMENT_NOT_FOUND, from, to, "", 0, 0);
				} else {
					if (mgmt.requiresRedirection == 1) {
						if (mgmt.redirectTo != null && !mgmt.redirectTo.equals("")) {
							insertMoRedirection(ctx, msg, mgmt.id);
						}
					}
					
					String ownerId = getMainCampaignOwner(ctx, mgmt.id);
					boolean managed = false;

					if (mgmt.detectDateRange == 1) {
						Date now = new Date();

						if (mgmt.campaignStartTime == null || mgmt.campaignEndTime == null) {
							inserted = insertMo(ctx, transId, keyword, msg, STATUS_DATE_RANGE_ERROR, from, to, "", mgmt.id, 0);
							managed = true;
						} else if (now.getTime() > mgmt.campaignEndTime.getTime()) {
							if (mgmt.campaignEndedMessage != null && !mgmt.campaignEndedMessage.equals("")) {
								String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.campaignEndedMessage);

								if (batchBroadcastId.equals("-1")) {
									inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "7", from, to, batchBroadcastId, mgmt.id, 0);
								}
							} else {
								inserted = insertMo(ctx, transId, keyword, msg, "7", from, to, "", mgmt.id, 0);
							}
							managed = true;

						}
					}

					if (!managed) {
						if (mgmt.preventDuplicate == 1) {
							if (isEntryDuplicated(ctx, mgmt.id, mgmt.keyword, to)) {
								if (mgmt.duplicateMessage != null && !mgmt.duplicateMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.duplicateMessage);
									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "2", from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "2", from, to, "", mgmt.id, 0);
								}
								managed = true;
							}
						}
					}
					
					if (!managed) {
						if (mgmt.filterUnicode == 1) {
							if (hasNonAsciiChars(msg)) {
								if (mgmt.unicodeErrorMessage != null && !mgmt.unicodeErrorMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.unicodeErrorMessage);
									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "8", from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "8", from, to, "", mgmt.id, 0);
								}
								managed = true;
							}
						}
					}

					if (!managed) {
						if (mgmt.strictSecondaryKeyword == 1) {
							if (inputArr.length < 2) {
								if (mgmt.secondaryKeywordErrorMessage != null && !mgmt.secondaryKeywordErrorMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.secondaryKeywordErrorMessage);

									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "1", from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "1", from, to, "", mgmt.id, 0);
								}
								managed = true;
							} else {
								if (!isSecondaryKeywordValid(ctx, mgmt.id, inputArr[1])) {
									if (mgmt.secondaryKeywordErrorMessage != null && !mgmt.secondaryKeywordErrorMessage.equals("")) {
										String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.secondaryKeywordErrorMessage);
										if (batchBroadcastId.equals("-1")) {
											inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
										} else {
											inserted = insertMo(ctx, transId, keyword, msg, "1", from, to, batchBroadcastId, mgmt.id, 0);
										}
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "1", from, to, "", mgmt.id, 0);
									}
									managed = true;
								}
							}
						}
					}

					if (!managed) {
						if (mgmt.fixInputLength == 1) {
							if (mgmt.inputLength != inputArr.length) {
								if (mgmt.inputLengthErrorMessage != null && !mgmt.inputLengthErrorMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.inputLengthErrorMessage);
									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "10", from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "10", from, to, "", mgmt.id, 0);
								}
								managed = true;
							}
						}
					}

					if (!managed) {
						if (mgmt.fixPattern == 1) {
							StringBuilder errorMessageBuilder = new StringBuilder();
							StringBuilder errorCodeBuilder = new StringBuilder();
							
							if (!validatePattern(inputArr, mgmt.pattern, errorMessageBuilder, errorCodeBuilder)) {
								String errorMessage = errorMessageBuilder.toString();
								String errorCode = errorCodeBuilder.toString();
								
								if (!errorMessage.equals("")) {
									mgmt.patternErrorMessage = errorMessage;
								}

								if (mgmt.patternErrorMessage != null && !mgmt.patternErrorMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.patternErrorMessage);
									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, errorCode, from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, errorCode, from, to, "", mgmt.id, 0);
								}
								managed = true;
							}
						}
					}
					
					if (!managed) {
						if (mgmt.checkOkLimit == 1) {
							int validCountSoFar = getTotalValidCount(ctx, mgmt.id);
							if (validCountSoFar >= mgmt.okLimit) {
								if (mgmt.exceedOkLimitMessage != null && !mgmt.exceedOkLimitMessage.equals("")) {
									String batchBroadcastId = insertAutoReply(ctx, to, ownerId, mgmt.exceedOkLimitMessage);
									if (batchBroadcastId.equals("-1")) {
										inserted = insertMo(ctx, transId, keyword, msg, STATUS_NO_CREDITS, from, to, batchBroadcastId, mgmt.id, 0);
									} else {
										inserted = insertMo(ctx, transId, keyword, msg, "12", from, to, batchBroadcastId, mgmt.id, 0);
									}
								} else {
									inserted = insertMo(ctx, transId, keyword, msg, "12", from, to, "", mgmt.id, 0);
								}
								managed = true;
							}
						}
					}

					if (!managed) {
						// At this point we assume nothing is wrong
						if (mgmt.autoReply == 1) {
							String messageToReply = mgmt.autoReplyMessage;
							if (mgmt.replyBySecondaryKeyword == 1) {
								messageToReply = getSecondayKeywordReply(ctx, mgmt.id, inputArr[1]);
							}
							
							if (messageToReply != null && !messageToReply.equals("")) {
								if (mgmt.replyBasedOnInput == 1) {
									if (messageToReply.contains("#input1#")) {
										if (inputArr.length >= 2) {
											logger.info("Will replace message from: " + messageToReply);
											messageToReply = messageToReply.replaceAll("#input1#", inputArr[1]);
											logger.info("To: " + messageToReply);
										} else {
											messageToReply = messageToReply.replaceAll("#input1#", "");
										}
									}
								}
							}
							
							if (messageToReply != null && !messageToReply.equals("")) {
								String batchBroadcastId = insertAutoReply(ctx, to, ownerId, messageToReply);
								inserted = insertMo(ctx, transId, keyword, msg, "0", from, to, batchBroadcastId, mgmt.id, 1);
							} else {
								inserted = insertMo(ctx, transId, keyword, msg, "0", from, to, "", mgmt.id, 1);
							}
							
							managed = true;
						} else {
							inserted = insertMo(ctx, transId, keyword, msg, "0", from, to, "", mgmt.id, 1);
						}
					}
				}
			}

			if (!inserted) {
				logger.error("Insertion error for " + transId);
			}

			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().write("OK");

			logger.info("Processed " + transId + " in " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			response.getWriter().write("Please retry");
		}
	}

	private boolean validatePattern(String[] inputArr, String pattern, StringBuilder errorMessageBuilder,
			StringBuilder errorCodeBuilder) {
		try {
			String[] patternArr = pattern.split(" ");

			if (patternArr.length > inputArr.length) {
				logger.info("Pattern Error: Length of pattern is more than length of input");
				errorCodeBuilder.append("6");
				return false;
			}

			for (int i = 0; i < patternArr.length; i++) {
				String thisPattern = patternArr[i].toLowerCase();

				if (thisPattern.charAt(0) != '-' && thisPattern.charAt(thisPattern.length() - 1) != '-'
						&& !thisPattern.equals("*")) {
					if (!inputArr[i].toLowerCase().equals(thisPattern.toLowerCase())) {
						logger.info("Pattern Error: " + inputArr[i] + " != " + thisPattern.toLowerCase());
						errorCodeBuilder.append("5");
						return false;
					}
				} else if (thisPattern.equals("*")) {
					continue;
				} else if (thisPattern.equals("-ic-")) {
					if (inputArr[i].length() != 12) {
						logger.info("Pattern Error: Ic is not 12 digits");
						errorMessageBuilder.append("RM0.00 Invalid IC Number");
						errorCodeBuilder.append("3");
						return false;
					}

					if (!isNumeric(inputArr[i])) {
						logger.info("Pattern Error: Ic is not numeric");
						errorMessageBuilder.append("RM0.00 Invalid IC Number");
						errorCodeBuilder.append("3");
						return false;
					}

				} else if (thisPattern.equals("-email-")) {
					if (!isValidEmailAddress(inputArr[i])) {
						logger.info("Pattern Error: Email is invalid");
						errorMessageBuilder.append("RM0.00 Invalid Email Address");
						errorCodeBuilder.append("4");
						return false;
					}
				}
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}

		return true;
	}

	private boolean isValidEmailAddress(String email) {
		EmailValidator validator = EmailValidator.getInstance();
		if (validator.isValid(email)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean isNumeric(String str) {
		for (char c : str.toCharArray()) {
			if (!Character.isDigit(c))
				return false;
		}
		return true;
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
	
	private int getTotalValidCount(ServletContext ctx, long campaignId) {
		int totalValid = 999999;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "SELECT COUNT(*) FROM bsg_mo WHERE mo_campaign_id = ? AND valid = 1 AND received_time >= ? AND received_time <= ?;";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();
			
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String todayDateString = sdf.format(cal.getTime());
			String startTime = todayDateString + " 00:00:00";
			String endTime = todayDateString + " 23:59:59";

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setLong(1, campaignId);
			stmt.setString(2, startTime);
			stmt.setString(3, endTime);

			rs = stmt.executeQuery();

			while (rs.next()) {
				totalValid = rs.getInt(1);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}

		return totalValid;
	}
	
	private boolean insertMoRedirection(ServletContext ctx, String content, long campaignId) {
		boolean succeed = false;

		Connection conn = null;
		PreparedStatement stmt = null;

		String query = "INSERT INTO bsg_mo_redirect (content, redirected, mo_campaign_id) "
				+ "VALUES (?, 0, ?);";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setString(1, content);
			stmt.setLong(2, campaignId);

			stmt.executeUpdate();
			succeed = true;
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(stmt, conn);
		}

		return succeed;
	}

	private boolean insertMo(ServletContext ctx, String transId, String keyword, String content, String status,
			String from, String to, String batchBroadcastId, long campaignId, int valid) {
		boolean succeed = false;

		Connection conn = null;
		PreparedStatement stmt = null;

		String query = "INSERT INTO bsg_mo (`trans_id`, `keyword`, `content`, `received_time`, `status`, `from`, `to`, `batch_broadcast_id`, `mo_campaign_id`, `valid`) "
				+ "VALUES (?, ?, ?, now(), ?, ?, ?, ?, ?, ?);";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setString(1, transId);
			stmt.setString(2, keyword);
			stmt.setString(3, content);
			stmt.setString(4, status);
			stmt.setString(5, from);
			stmt.setString(6, to);
			stmt.setString(7, batchBroadcastId);
			stmt.setLong(8, campaignId);
			stmt.setInt(9, valid);

			stmt.executeUpdate();
			succeed = true;
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(stmt, conn);
		}

		return succeed;
	}

	private MoManagement getManagementDetails(ServletContext ctx, String keyword) {
		MoManagement mgmt = null;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "SELECT `id`, `keyword`, `created_date`, `auto_reply`, `auto_reply_message`, "
				+ "`prevent_duplicate_keyword`, `duplicate_message`, `detect_date_range`, `campaign_not_started_message`, "
				+ "`campaign_ended_message`, `campaign_start_time`, `campaign_end_time`, `campaign_owner_group_id`, `status`, "
				+ "`strict_secondary_keyword`, `secondary_keyword_error_message`, `fix_input_length`, `input_length_error_message`, "
				+ "`input_length`, `fix_pattern`, `pattern`, `pattern_error_message`, `requires_redirection`, `redirect_to`, "
				+ "`filter_unicode`, `unicode_error_message`, `reply_by_secondary_keyword`, "
				+ "`check_ok_limit`, `ok_limit`, `exceed_ok_limit_message`, `reply_based_on_input` "
				+ "FROM  `bsg_mo_campaign` WHERE keyword = ? and status = 0 and campaign_start_time < now() order by campaign_end_time desc limit 1;";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setString(1, keyword);

			rs = stmt.executeQuery();

			while (rs.next()) {
				mgmt = new MoManagement();
				mgmt.id = rs.getLong(1);
				mgmt.keyword = rs.getString(2);
				mgmt.createdDate = rs.getTimestamp(3);
				mgmt.autoReply = rs.getInt(4);
				mgmt.autoReplyMessage = rs.getString(5);

				mgmt.preventDuplicate = rs.getInt(6);
				mgmt.duplicateMessage = rs.getString(7);
				mgmt.detectDateRange = rs.getInt(8);
				mgmt.campaignNotStartedMessage = rs.getString(9);

				mgmt.campaignEndedMessage = rs.getString(10);
				mgmt.campaignStartTime = rs.getTimestamp(11);
				mgmt.campaignEndTime = rs.getTimestamp(12);
				mgmt.campaignOwnerGroup = rs.getInt(13);
				mgmt.status = rs.getInt(14);

				mgmt.strictSecondaryKeyword = rs.getInt(15);
				mgmt.secondaryKeywordErrorMessage = rs.getString(16);
				mgmt.fixInputLength = rs.getInt(17);
				mgmt.inputLengthErrorMessage = rs.getString(18);

				mgmt.inputLength = rs.getInt(19);
				mgmt.fixPattern = rs.getInt(20);
				mgmt.pattern = rs.getString(21);
				mgmt.patternErrorMessage = rs.getString(22);
				
				mgmt.requiresRedirection = rs.getInt(23);
				mgmt.redirectTo = rs.getString(24);
				
				mgmt.filterUnicode = rs.getInt(25);
				mgmt.unicodeErrorMessage = rs.getString(26);
				
				mgmt.replyBySecondaryKeyword = rs.getInt(27);
				
				mgmt.checkOkLimit = rs.getInt(28);
				mgmt.okLimit = rs.getInt(29);
				mgmt.exceedOkLimitMessage = rs.getString(30);
				
				mgmt.replyBasedOnInput = rs.getInt(31);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}

		return mgmt;
	}
	
	private String getSecondayKeywordReply(ServletContext ctx, long campaignId, String secondaryKeyword) {
		String message = "";
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "select valid_reply from `bsg_mo_secondary_keyword` where mo_campaign_id = ? and secondary_keyword = ?";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setLong(1, campaignId);
			stmt.setString(2, secondaryKeyword);

			rs = stmt.executeQuery();

			while (rs.next()) {
				message = rs.getString(1);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}
		return message;
	}

	private boolean isSecondaryKeywordValid(ServletContext ctx, long campaignId, String secondaryKeyword) {
		boolean valid = false;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "select 1 from `bsg_mo_secondary_keyword` where mo_campaign_id = ? and secondary_keyword = ?";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setLong(1, campaignId);
			stmt.setString(2, secondaryKeyword);

			rs = stmt.executeQuery();

			while (rs.next()) {
				valid = true;
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}
		return valid;
	}

	private boolean isEntryDuplicated(ServletContext ctx, long campaignId, String keyword, String msisdn) {
		boolean valid = false;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "select 1 from `bsg_mo` where mo_campaign_id = ? and keyword = ? and `to` = ? and `valid` = 1";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setLong(1, campaignId);
			stmt.setString(2, keyword);
			stmt.setString(3, msisdn);

			rs = stmt.executeQuery();

			while (rs.next()) {
				valid = true;
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}
		return valid;
	}

	private String getMainCampaignOwner(ServletContext ctx, long ownerGroupId) {
		String ownerId = "";

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "SELECT customer_id FROM bsg_mo_owner_group "
				+ "LEFT JOIN bsg_client_usermaster ON bsg_mo_owner_group.user_id = bsg_client_usermaster.id "
				+ "WHERE main = 1 AND bsg_mo_owner_group.campaign_id = ? LIMIT 1";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setLong(1, ownerGroupId);

			rs = stmt.executeQuery();

			while (rs.next()) {
				ownerId = rs.getString(1);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}

		return ownerId;
	}

	private String insertAutoReply(ServletContext ctx, String msisdn, String fromAccount, String message) {
		int batchBroadcastId = getNewBroadcastId(ctx);
		int creditPerSms = getMalaysiaCreditPerSms(ctx);
		StringBuilder msgTypeBuilder = new StringBuilder();
		int numberOfSms = getNumberOfSMS(message, msgTypeBuilder);
		int amount = numberOfSms * creditPerSms;
		String msgType = msgTypeBuilder.toString();
		String mtUuid = UUID.randomUUID().toString();
		CreditDetails creditDetails = new CreditDetails();

		logger.info("Batch: " + batchBroadcastId + ", cps: " + creditPerSms + ", sms count: " + numberOfSms
				+ ", amount:" + amount + ", type: " + msgType + ", msg: " + message);

		boolean deducted = false;
		try {
			deducted = deductCredits(ctx, amount, fromAccount, creditDetails);
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
		if (!deducted) {
			return "-1";
		}

		Connection conn = null;
		PreparedStatement mtStmt = null;
		PreparedStatement batchStmt = null;

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			String insertMtQuery = "Insert Into bsg_sms_log (BD_BATCHID,CUSTOMER_ID,AMOUNT,SENDER_ID,SMS_COUNT,TO_MSISDN,MESSAGE_TEXT,"
					+ "SENT_STATUS,DELIVERY_STATUS,BATCH_BROADCAST_ID,"
					+ "MESSAGE_TYPE,Batch_ID_Initial,Sms_Account,Batch_Index) Values "
					+ "(?, ?, ?, '68886', ?, ?, ?, 'N', 'N', ?, ?, '0', "
					+ "(SELECT IFNULL(sms_account,'websms1') FROM bsg_client_usermaster WHERE customer_id = ?), 0)";

			mtStmt = conn.prepareStatement(insertMtQuery);
			mtStmt.setString(1, mtUuid);
			mtStmt.setString(2, fromAccount);
			mtStmt.setInt(3, amount);
			mtStmt.setInt(4, numberOfSms);
			mtStmt.setString(5, msisdn);
			mtStmt.setString(6, message);
			mtStmt.setString(7, Integer.toString(batchBroadcastId));
			mtStmt.setString(8, msgType);
			mtStmt.setString(9, fromAccount);
			mtStmt.executeUpdate();

			String insertBatchQuery = "INSERT INTO bsg_sms_transaction (BATCH_BROADCAST_ID, BD_TYPE, CREATED_BY, CREATED_DATE, "
					+ "CURRENT_BALANCE, CUSTOMER_ID, FAILURE_COUNT, MESSAGE_TEXT, MESSAGE_TYPE, "
					+ "PENDING_COUNT, PREVIOUS_BALANCE, SENDER_ID, SENT_STATUS, SMS_COUNT, "
					+ "SUCCESS_COUNT, TOTAL_CREDITS, TOTAL_RECIPIENTS, TRANSFER_DATE, USER_NAME) Values "
					+ "(?, 2, (SELECT username FROM bsg_client_usermaster WHERE customer_id = ?), now(), "
					+ "?, ?, 0, ?, ?, 1, ?, '68886', 'P', 1, 0, ?, 1, now(), "
					+ "(SELECT username FROM bsg_client_usermaster WHERE customer_id = ?))";

			batchStmt = conn.prepareStatement(insertBatchQuery);
			batchStmt.setString(1, Integer.toString(batchBroadcastId));
			batchStmt.setString(2, fromAccount);
			batchStmt.setInt(3, creditDetails.newAmount);
			batchStmt.setString(4, fromAccount);
			batchStmt.setString(5, message);
			batchStmt.setString(6, msgType);
			batchStmt.setInt(7, creditDetails.previousAmount);
			batchStmt.setInt(8, amount);
			batchStmt.setString(9, fromAccount);
			batchStmt.executeUpdate();

		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(batchStmt, mtStmt, conn);
		}

		return Integer.toString(batchBroadcastId);
	}

	private static synchronized boolean deductCredits(ServletContext ctx, int amount, String fromAccount,
			CreditDetails creditDetails) {
		boolean deducted = false;
		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			int remainingCredits = getRemainingCredit(ctx, fromAccount);
			logger.info(fromAccount + " has " + remainingCredits + " credits, need to deduct " + amount);

			if (remainingCredits < amount) {
				return false;
			}

			int newAmount = remainingCredits - amount;

			creditDetails.newAmount = newAmount;
			creditDetails.previousAmount = remainingCredits;

			logger.info(fromAccount + " new amount is " + newAmount);

			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			String updateQuery = "update bsg_credit set TOTAL_CREDIT_USED = ?, NEW_BALANCE = ?, "
					+ "PREVIOUS_BALANCE = ? where CUSTOMER_ID = ?";

			stmt = conn.prepareStatement(updateQuery);
			stmt.setInt(1, amount);
			stmt.setInt(2, newAmount);
			stmt.setInt(3, remainingCredits);
			stmt.setString(4, fromAccount);

			stmt.executeUpdate();

			deducted = true;
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(stmt, conn);
		}

		return deducted;
	}

	private static synchronized int getRemainingCredit(ServletContext ctx, String fromAccount) {
		int remainingCredits = 0;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String query = "SELECT CLIENTCREDIT_ID, CUSTOMER_ID, CUSTOMER_CODE, PACKAGE_ID, PURCHASE_DATE, VALIDITY_PERIOD, "
					+ "EXPIRY_DATE, CUMULATIVE_TOTAL, NEW_BALANCE, PREVIOUS_BALANCE, REMARKS, CREATED_DATE, CREATED_BY, "
					+ "LAST_UPDATED_DATE, LAST_UPDATED_BY, TOTAL_CREDIT_USED "
					+ "FROM bsg_credit WHERE CUSTOMER_ID = ?";

			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);
			stmt.setString(1, fromAccount);

			rs = stmt.executeQuery();

			while (rs.next()) {
				remainingCredits = rs.getInt(9);
			}

		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}

		return remainingCredits;
	}

	private int getNumberOfSMS(String message, StringBuilder msgType) {
		int count = 0;
		try {
			if (message != null && message.trim() != "" && message.length() > 0) {
				String value = message;
				int str = value.length();
				int nextMsgLength = 0;
				int actualMsgLength = 0;
				int specialCharLength = 0;
				String messageType = null;
				int msgLength = 160;
				int returnCount = 0;
				for (int i = 0; str > i; i++) {
					int charactercode = Character.codePointAt(value, i);
					if ((charactercode >= 32 && charactercode <= 126 && charactercode != 96) || charactercode == 10 || charactercode == 13) {
						actualMsgLength = msgLength;
						nextMsgLength = 153;
						messageType = "ascii";
					} else {
						msgLength = 70;
						actualMsgLength = msgLength;
						nextMsgLength = 67;
						messageType = "unicode";
						break;
					}
				}
				for (int i = 0; str > i; i++) {
					int charactercode = Character.codePointAt(value, i);
					if ((charactercode >= 91 && charactercode <= 94)
							|| (charactercode >= 123 && charactercode <= 126)) {
						if (messageType.equalsIgnoreCase("ascii")) {
							specialCharLength = specialCharLength + 1;
						}
					}
					if (charactercode == 13) {
						returnCount++;
					}
				}
				int messageLength = value.length() + specialCharLength - returnCount;
				if (messageLength > actualMsgLength) {
					msgLength = nextMsgLength;
				}
				int chars = messageLength;
				float messages = (float) chars / msgLength;
				count = (int) Math.ceil(messages);

				msgType.append(messageType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return count;
	}

	private int getMalaysiaCreditPerSms(ServletContext ctx) {
		int credit = 0;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "SELECT credit_per_sms FROM bsg_sms_credit_count WHERE country_code = 60;";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query);

			rs = stmt.executeQuery();

			while (rs.next()) {
				credit = rs.getInt(1);
			}

			logger.info("Malaysia cps: " + credit);
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(stmt, conn);
		}
		return credit;
	}

	private int getNewBroadcastId(ServletContext ctx) {
		int broadcastId = 0;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String query = "INSERT INTO `batch_broadcast_id` (`id`) VALUES (NULL);";

		try {
			String driver = "com.mysql.jdbc.Driver";
			Class.forName(driver).newInstance();

			conn = DriverManager.getConnection(ctx.getInitParameter("dbUrl"), ctx.getInitParameter("dbUser"),
					ctx.getInitParameter("dbPass"));

			stmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

			stmt.executeUpdate();

			rs = stmt.getGeneratedKeys();
			if (rs != null && rs.next()) {
				broadcastId = rs.getInt(1);
			}

			logger.info("Created batch ID: " + broadcastId);
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			tryClose(rs, stmt, conn);
		}
		return broadcastId;
	}
	
	public boolean hasNonAsciiChars(String message) {
		try {
			if (message != null && message.trim() != "" && message.length() > 0) {
				for (int i = 0; i < message.length(); i++) {
					int charactercode = Character.codePointAt(message, i);
					if ((charactercode >= 32 && charactercode <= 126 && charactercode != 96) || charactercode == 10 || charactercode == 13) {
						
					} else {
						return true;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	private static void tryClose(Object... objs) {
		try {
			for (Object obj : objs) {
				if (obj == null) {
					continue;
				}

				try {
					if (obj instanceof ResultSet) {
						((ResultSet) obj).close();
					}
					if (obj instanceof CallableStatement) {
						((CallableStatement) obj).close();
					}
					if (obj instanceof PreparedStatement) {
						((PreparedStatement) obj).close();
					}
					if (obj instanceof Connection) {
						((Connection) obj).close();
					}
				} catch (Exception ex) {
					logger.error(ex.getMessage());
					logger.error(ex.getStackTrace().toString());
				}
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			ex.printStackTrace();
		}
	}

}
