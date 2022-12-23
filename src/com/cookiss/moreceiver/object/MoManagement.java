package com.cookiss.moreceiver.object;

import java.util.Date;

public class MoManagement {
	public long id;
	public String keyword;
	public Date createdDate;
	public int autoReply;
	public String autoReplyMessage;
	public int preventDuplicate;
	public String duplicateMessage;
	public int detectDateRange;
	public String campaignNotStartedMessage;
	public String campaignEndedMessage;
	public Date campaignStartTime;
	public Date campaignEndTime;
	public int campaignOwnerGroup;
	public int status;
	public int strictSecondaryKeyword;
	public String secondaryKeywordErrorMessage;
	public int fixInputLength;
	public int inputLength;
	public String inputLengthErrorMessage;
	public int fixPattern;
	public String pattern;
	public String patternErrorMessage;
	public int requiresRedirection;
	public String redirectTo;
	public int filterUnicode;
	public String unicodeErrorMessage;
	public int replyBySecondaryKeyword;
	public int checkOkLimit;
	public int okLimit;
	public String exceedOkLimitMessage;
	public int replyBasedOnInput;
}
