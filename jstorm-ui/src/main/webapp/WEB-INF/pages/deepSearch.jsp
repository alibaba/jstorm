<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  --%>

<html>
<head>
    <jsp:include page="layout/_head.jsp"/>
</head>
<body>
<jsp:include page="layout/_header.jsp"/>

<div class="container-fluid">
    <h2>
        Deep Search for topology:
        <code class="path">
            ${topologyId}
        </code>
    </h2>

    <div class="form-inline">
        <form class="form-group" action="deepSearch" method="get">
            <input type="hidden" name="cluster" value="${clusterName}">
            <input type="hidden" name="port" value="${logServerPort}">
            <input type="hidden" name="tid" value="${topologyId}">

            <div class="form-group">
                <label>Search:</label>

                <div class="input-group">
                    <input type="text" class="form-control" name="key" value="${keyword}">
          <span class="input-group-btn">
            <button class="btn btn-default" type="submit">Search</button>
          </span>
                </div><!-- /input-group -->
            </div>
            <div class="checkbox">
                <label>
                    <input type="checkbox" name="caseIgnore" ${caseIgnore == 'true' ? "checked" : ""} > Case Ignore
                </label>
            </div>
        </form>
    </div>

    <hr/>
    <c:choose>
        <c:when test="${tip != null}">
            <div class="col-md-8 col-md-offset-2 alert alert-warning" role="alert">
                <strong>Ooops!</strong> ${tip}
            </div>
        </c:when>
        <c:otherwise>
            <table class="table sortable">
                <thead>
                <tr>
                    <th>Host:Port</th>
                    <th>Offset</th>
                    <th>Match</th>
                </tr>
                </thead>
                <tbody id="html-data">
                <c:forEach var="r" items="${result}">
                    <c:forEach var="m" items="${r.match}">
                        <tr>
                            <td>
                                ${r.host}:${r.port}
                            </td>
                            <td>
                                <a href="log?cluster=${clusterName}&host=${r.host}&port=${logServerPort}&file=${r.file}&dir=${r.dir}&pos=${m.key}"
                                   target="_blank">
                                        ${m.key}
                                </a>
                            </td>
                            <td>
                                <pre>${m.value}</pre>
                            </td>
                        </tr>
                    </c:forEach>
                </c:forEach>
                </tbody>
            </table>
        </c:otherwise>
    </c:choose>
</div>

<jsp:include page="layout/_footer.jsp"/>
<script src="assets/js/hilitor.js"></script>
<script>
    $(function () {
        $('[data-toggle="tooltip"]').tooltip();

        $(".table").each(function () {
            $(this).DataTable({
                "info": true,
                "paging": true,
                "ordering": true,
                "searching": true,
                "order": [1, "desc"],
                "lengthMenu": [[15, 25, 50, -1], [15, 25, 50, "ALL"]]
            });
        });

        var myHilitor = myHilitor = new Hilitor("html-data");
        myHilitor.apply("${keyword}", true, ${caseIgnore});
    });
</script>
</body>
</html>