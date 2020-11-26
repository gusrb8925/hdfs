<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>1초마다 URL를 호출하여 로그를 생성하는 JSP 페이지이다.</title>
<script src="/js/jquery-3.5.1.min.js"></script>
<script type="text/javascript">
	var logCnt = 0; // 로그 찍은 함수 저장을 위한 변수
	
	// JSP 페이지가 완전히 로딩완료(</html> 태그까지 실행 완료) 되면 1번만 자동실행
	$(window).on("load",function(){
		
		// 화면 로딩이 완료되면 첫번째로 실행함
		doRequestURL();
		
		// 2번째로부터 채팅방 전체리스트를 1초마다 로딩
		// 컴퓨터 밀리세컨드(10^3제곱) 단위로 처리하기 때문에 1000 > 1초
		setInterval("doRequestURL();",1000);
		
	});
	
	// URL 호출하기
	function doRequestURL() {
		
		logCnt++; // 로그 호출수 1씩 증가
		
		// Ajax 호출
		$.ajax({
			url : "/log/result.jsp?logCnt="+logCnt,
			type : "post",
			dataType : "JSON",
			contentType : "application/json; charset=UTF-8",
			success : function(json) {
				$("#logView").html("My Create logMessage : "+json.logMessage);
			}
		})
	}
</script>
</head>
<body>
	<hr/>
	<br/>
	<h2>1초마다 URL를 호출하여 로그 생성하는 페이지!!</h2>
	<br/>
	<hr/>
	<div id="logView">[결과표시]</div>
</body>
</html>