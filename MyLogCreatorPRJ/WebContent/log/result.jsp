<%@ page language="java" contentType="text/json; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ page trimDirectiveWhitespaces="true"%>
<%
	// 로그 찍은 회수
	String logCnt = request.getParameter("logCnt");

	// 로그 찍을 문구
	String logMessage = "Log Number[" + logCnt + "]";
	
	// 로그 찍기
	System.out.println("logMessage : " + logMessage);
%>    
{"logMessage":"<%=logMessage %>"}