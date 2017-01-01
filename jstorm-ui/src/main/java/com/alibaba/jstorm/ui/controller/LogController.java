/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.ui.controller;

import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.ui.model.Response;
import com.alibaba.jstorm.ui.model.UIWorkerMetric;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class LogController {
    private static final Logger LOG = LoggerFactory.getLogger(LogController.class);

    private static final long MAX_DOWNLOAD_SIZE = 10 * 1024 * 1024; //10MB
    private static final int BLOCK_DOWNLOAD_SIZE = 1024 * 1024;     //block size of every download request 1M
    private static final int KEY_WORD_MIN_LENGTH = 2;

    private static ThreadPoolExecutor _backround = new ThreadPoolExecutor(0,Integer.MAX_VALUE, 60L,TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    @RequestMapping(value = "/log", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "port", required = false) String logServerPort,
                       @RequestParam(value = "dir", required = false) String dir,
                       @RequestParam(value = "wport", required = false) String workerPort,
                       @RequestParam(value = "file", required = false) String logName,
                       @RequestParam(value = "tid", required = false) String topologyId,
                       @RequestParam(value = "pos", required = false) String pos,
                       ModelMap model) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        topologyId = StringEscapeUtils.escapeHtml(topologyId);
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }
        Map conf = UIUtils.getNimbusConf(clusterName);
        Event event = new Event(clusterName, host, logServerPort, logName, topologyId, workerPort, pos, dir, conf);

        try {
            requestLog(event, model);
        } catch (IOException e) {
            e.printStackTrace();
        }


        model.addAttribute("logName", event.logName);
        model.addAttribute("host", host);
        model.addAttribute("dir", event.dir);
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("logServerPort", logServerPort);
        model.addAttribute("topologyId", topologyId);
        model.addAttribute("fullFile", getFullFile(event.dir, event.logName));
        model.addAttribute("workerPort", workerPort);
        return "log";
    }

    @RequestMapping(value = "/logSearch", method = RequestMethod.GET)
    public String search(@RequestParam(value = "cluster", required = true) String clusterName,
                         @RequestParam(value = "host", required = true) String host,
                         @RequestParam(value = "port", required = true) String logServerPort,
                         @RequestParam(value = "dir", required = true) String dir,
                         @RequestParam(value = "file", required = true) String logName,
                         @RequestParam(value = "key", required = false) String keyword,
                         @RequestParam(value = "workerPort", required = false) String workerPort,
                         @RequestParam(value = "tid", required = false) String topologyId,
                         @RequestParam(value = "pos", required = false) String pos,
                         @RequestParam(value = "caseIgnore", required = false) String caseIgnore,
                         ModelMap model) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        topologyId = StringEscapeUtils.escapeHtml(topologyId);
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }
        String fullFile = getFullFile(dir, logName);
        boolean _caseIgnore = !StringUtils.isBlank(caseIgnore);
        model.addAttribute("keyword", keyword);
        try {
            keyword = URLEncoder.encode(keyword, "UTF-8");      // encode space and url characters
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }
        String url = String.format("http://%s:%s/logview?cmd=searchLog&file=%s&key=%s&offset=%s&case_ignore=%s",
                host, logServerPort, fullFile, keyword, JStormUtils.parseLong(pos, 0), _caseIgnore);

        if (filterKeyword(model, keyword)) {
            try {
                HttpResponse httpResponse = httpGet(url);
                String data = EntityUtils.toString(httpResponse.getEntity());
                Map result = (Map) JStormUtils.from_json(data);
                if (result == null) {
                    model.addAttribute("tip", data);
                } else if (result.get("error") != null) {
                    model.addAttribute("tip", result.get("msg"));
                } else {
                    model.addAttribute("matchResults", result.get("match_results"));
                    model.addAttribute("nextOffset", result.get("next_offset"));
                    model.addAttribute("numMatch", result.get("num_match"));
                }
            } catch (IOException e) {
                e.printStackTrace();
                model.addAttribute("tip", "Internal Error, can't get response from logview");
            }
        }

        model.addAttribute("host", host);
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("logServerPort", logServerPort);
        model.addAttribute("topologyId", topologyId);
        model.addAttribute("workerPort", workerPort);
        model.addAttribute("dir", dir);
        model.addAttribute("file", logName);
        model.addAttribute("caseIgnore", _caseIgnore);
        UIUtils.addTitleAttribute(model, "LogSearch");
        return "logSearch";
    }

    class SearchRequest implements Callable<Void> {

        private String url;
        private String host;
        private String port;
        private String dir;
        private String file;
        private ConcurrentLinkedQueue<Map> result;

        public SearchRequest(String url, String host, String port, String dir, String file, ConcurrentLinkedQueue<Map> result) {
            this.url = url;
            this.host = host;
            this.port = port;
            this.dir = dir;
            this.file = file;
            this.result = result;
        }

        @Override
        public Void call() throws Exception {
            try {
                HttpResponse httpResponse = httpGet(url);
                String data = EntityUtils.toString(httpResponse.getEntity());
                Map matchResult = (Map) JStormUtils.from_json(data);
                Map<String, Object> res = new HashMap<>();
                if (matchResult != null && matchResult.get("error") == null){
                    res.put("match", matchResult.get("match_results"));
                    res.put("host", host);
                    res.put("port", port);
                    res.put("dir", dir);
                    res.put("file", file);

                }
                result.add(res);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    @RequestMapping(value = "/deepSearch", method = RequestMethod.GET)
    public String deepSearch(@RequestParam(value = "cluster", required = true) String clusterName,
                             @RequestParam(value = "tid", required = true) String topologyId,
                             @RequestParam(value = "key", required = false) String keyword,
                             @RequestParam(value = "caseIgnore", required = false) String caseIgnore,
                             ModelMap model) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        topologyId = StringEscapeUtils.escapeHtml(topologyId);
        boolean _caseIgnore = !StringUtils.isBlank(caseIgnore);
        int port = UIUtils.getSupervisorPort(clusterName);
        model.addAttribute("keyword", keyword);
        List<Future<?>> futures = new ArrayList<>();
        ConcurrentLinkedQueue<Map> result = new ConcurrentLinkedQueue<>();

        if (filterKeyword(model, keyword)) {
            NimbusClient client = null;
            try {
                keyword = URLEncoder.encode(keyword, "UTF-8");      // encode space and url characters
                client = NimbusClientManager.getNimbusClient(clusterName);
                TopologyInfo info = client.getClient().getTopologyInfo(topologyId);
                String topologyName = info.get_topology().get_name();
                List<UIWorkerMetric> workerData = UIMetricUtils.getWorkerMetrics(info.get_metrics().get_workerMetric(), topologyId, 60);
                String dir = "." + File.separator +  topologyName;
                for (UIWorkerMetric metric : workerData){
                    String logFile = topologyName + "-worker-" + metric.getPort() + ".log";
                    String url = String.format("http://%s:%s/logview?cmd=searchLog&file=%s&key=%s&offset=%s&case_ignore=%s",
                            metric.getHost(), port, getFullFile(dir, logFile), keyword, 0, _caseIgnore);
                    futures.add(_backround.submit(new SearchRequest(url, metric.getHost(), metric.getPort(), dir, logFile, result)));
                }

                JStormServerUtils.checkFutures(futures);

                model.addAttribute("result", result);

            } catch (NotAliveException nae) {
                model.addAttribute("tip", String.format("The topology: %s is dead.", topologyId));
            } catch (Exception e) {
                NimbusClientManager.removeClient(clusterName);
                LOG.error(e.getMessage(), e);
                UIUtils.addErrorAttribute(model, e);
            }
        }

        model.addAttribute("clusterName", clusterName);
        model.addAttribute("topologyId", topologyId);
        model.addAttribute("logServerPort", port);
        model.addAttribute("caseIgnore", _caseIgnore);
        UIUtils.addTitleAttribute(model, "DeepSearch");

        return "deepSearch";
    }

    private boolean filterKeyword(ModelMap model, String keyword) {
        if (!StringUtils.isBlank(keyword)) {
            if (keyword.length() > KEY_WORD_MIN_LENGTH) {
                return true;
            } else {
                model.addAttribute("tip", "The keyword length must larger than 2");
                return false;
            }
        }
        // skip search for empty keyword
        return false;
    }

    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public void download(@RequestParam(value = "host", required = true) String host,
                         @RequestParam(value = "port", required = false) String logServerPort,
                         @RequestParam(value = "dir", required = false) String dir,
                         @RequestParam(value = "file", required = false) final String filename,
                         HttpServletResponse response) {
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }

        String fullFile = getFullFile(dir, filename);

        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=" + filename);  //popup download file dialog

        String downloadUrl = String.format("http://%s:%s/logview?cmd=download&log=%s&size=%s", host, logServerPort, fullFile, BLOCK_DOWNLOAD_SIZE);
        String sizeUrl = String.format("http://%s:%s/logview?cmd=showLog&log=%s&pos=0&size=0", host, logServerPort, fullFile);

        try {
            long position = 0;
            long fileSize;
            HttpResponse httpResponse = httpGet(sizeUrl);
            if (httpResponse != null && httpResponse.getStatusLine().getStatusCode() == 200) {
                byte[] size = new byte[16];
                httpResponse.getEntity().getContent().read(size);
                fileSize = JStormUtils.parseLong(new String(size).trim(), MAX_DOWNLOAD_SIZE);
                // make sure download file size is less than max size
                if (fileSize - position > MAX_DOWNLOAD_SIZE) {
                    position = fileSize - MAX_DOWNLOAD_SIZE;
                }
            } else {
                handlFailure(response, "Bad request, can not read the file");
                return;
            }

            response.setContentLength((int) (fileSize - position));
            OutputStream os = response.getOutputStream();

            do {
                httpResponse = httpGet(downloadUrl + "&pos=" + position);
                int status = httpResponse.getStatusLine().getStatusCode();
                if (status == 200) {
                    httpResponse.getEntity().writeTo(os);
                    Header[] header = httpResponse.getHeaders("Content-Length");
                    int contentLength = JStormUtils.parseInt(header[0].getValue(), BLOCK_DOWNLOAD_SIZE);
                    position += contentLength;
                } else {
                    break;
                }
            } while (position < fileSize);

            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private String getFullFile(String dir, String filename) {
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }
        String fullFile;
        if (StringUtils.isBlank(dir)) {
            fullFile = filename;
        } else {
            fullFile = dir + File.separator + filename;
        }
        return fullFile;
    }


    private void handlFailure(HttpServletResponse response, String errorMsg) throws IOException {
        LOG.error(errorMsg);

        byte[] data = errorMsg.getBytes();
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentLength(data.length);
        OutputStream os = response.getOutputStream();
        os.write(data);
        os.close();
    }

    private HttpResponse httpGet(String url) throws IOException {
        HttpGet get = new HttpGet(url);
        HttpClient httpclient = HttpClientBuilder.create().build();
        return httpclient.execute(get);
    }


    private long getCurrentPageIndex(Event e, long totalSize, int pageSize) {
        long currentPos = totalSize;
        if (e.pos >= 0) {
            currentPos = e.pos;
        }

        return currentPos / pageSize;
    }

    private Pagination createPage(Event e, long index, int pageSize, String text,
                                  boolean isDisable, boolean isActive) {
        long pos = index * pageSize;
        String url = String.format("log?cluster=%s&host=%s&port=%s&file=%s&pos=%s&dir=%s",
                e.clusterName, e.host, e.logServerPort, e.logName, pos, e.dir);

        Pagination page = new Pagination();
        page.url = url;
        page.text = text;
        if (isDisable) {
            page.status = "disabled";
        } else if (isActive) {
            page.status = "active";
        }
        return page;
    }

    private List<Pagination> genPageUrl(Event e, String sizeStr) {
        long totalSize = Long.valueOf(sizeStr);
        int pageSize = e.logPageSize;

        long pageNum = (totalSize + pageSize - 1) / pageSize;
        long currentPageIndex = getCurrentPageIndex(e, totalSize, pageSize);

        List<Pagination> pages = new ArrayList<>();
        if (pageNum <= 10) {
            for (long i = pageNum - 1; i >= 0; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
            return pages;
        }

        if (pageNum - currentPageIndex < 5) {
            for (long i = pageNum - 1; i >= currentPageIndex; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        } else {
            pages.add(createPage(e, pageNum - 1, pageSize, "End", false, pageNum - 1 == currentPageIndex));
            pages.add(createPage(e, currentPageIndex + 4, pageSize, "...", false, false));
            for (long i = currentPageIndex + 3; i >= currentPageIndex; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        }

        if (currentPageIndex < 5) {
            for (long i = currentPageIndex - 1; i > 0; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        } else {
            for (long i = currentPageIndex - 1; i >= currentPageIndex - 3; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
            pages.add(createPage(e, currentPageIndex - 4, pageSize, "...", false, false));
            pages.add(createPage(e, 0, pageSize, "Begin", false, 0 == currentPageIndex));
        }
        return pages;
    }

    private void requestLog(Event e, ModelMap model) throws IOException {
        String fullPath;
        if (e.dir == null || e.dir.equals(".")) {
            fullPath = e.logName;
        } else {
            fullPath = e.dir + File.separator + e.logName;
        }
        String summary = null;
        String log = null;
        if (fullPath.contains("/..") || fullPath.contains("../")) {
            summary = "File Path can't contains <code>..</code> <br/>";
        } else {
            Response res = UIUtils.getLog(e.host, e.logServerPort, fullPath, e.pos);

            if (res.getStatus() != -1) {
                if (res.getStatus() == 200) {
                    String sizeStr = res.getData().substring(0, 16);
                    model.addAttribute("pages", genPageUrl(e, sizeStr));
                    log = res.getData().substring(17);
                } else {
                    summary = "The log file <code>" + e.logName + "</code> isn't exist <br/> " + res.getData();
                }
            } else {
                summary = "Failed to get log file <code>" + e.logName + "</code> <br/>" + res.getData();
            }
        }
        model.addAttribute("log", log);
        model.addAttribute("summary", summary);
        UIUtils.addTitleAttribute(model, "Log");
    }


    public class Event {

        public String clusterName;
        public String host;
        public int logServerPort;
        public String logName;
        public String topologyId;
        public String workerPort;
        public long pos;
        public String dir;
        public int logPageSize;
        public String logEncoding;


        public Event(String _clusterName, String _host, String _logServerPort, String _logName,
                     String _topologyId, String _workerPort, String _pos, String _dir, Map conf) {
            this.clusterName = _clusterName;
            this.host = _host;
            this.logServerPort = JStormUtils.parseInt(_logServerPort, 0);
            this.logName = _logName;
            this.topologyId = _topologyId;
            this.workerPort = _workerPort;
            this.pos = JStormUtils.parseLong(_pos, -1);
            this.dir = _dir;

            logPageSize = ConfigExtension.getLogPageSize(conf);
            logEncoding = ConfigExtension.getLogViewEncoding(conf);

            if (host == null) {
                throw new IllegalArgumentException("Please set host");
            } else if (logServerPort == 0) {
                throw new IllegalArgumentException(
                        "Please set log server's port");
            }

            if (!StringUtils.isBlank(logName)) {
                return;
            }

            if (topologyId == null || workerPort == null) {
                throw new IllegalArgumentException(
                        "Please set log fileName or topologyId-workerPort");
            }

            String topologyName;
            try {
                topologyName = Common.topologyIdToName(topologyId);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Please set log fileName or topologyId-workerPort");
            }
            dir = "." + File.separator + topologyName;
            logName = topologyName + "-worker-" + workerPort + ".log";
        }
    }


    public class Pagination {
        public String status;
        public String url;
        public String text;

        public Pagination(String status, String url, String text) {
            this.status = status;
            this.url = url;
            this.text = text;
        }

        public Pagination() {
        }

        public String getStatus() {
            return status;
        }

        public String getUrl() {
            return url;
        }

        public String getText() {
            return text;
        }
    }
}
