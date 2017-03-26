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
package com.alibaba.jstorm.daemon.supervisor;

import backtype.storm.Constants;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.Worker;
import com.alibaba.jstorm.utils.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
public class Httpserver implements Shutdownable {

    private static Logger LOG = LoggerFactory.getLogger(Httpserver.class);

    private HttpServer hs;
    private int port;
    private Map conf;

    public Httpserver(int port, Map conf) {
        this.port = port;
        this.conf = conf;
    }

    static class LogHandler implements HttpHandler {
        private String logDir;
        private String stormHome;
        private ArrayList<String> accessDirs = new ArrayList<String>();
        Map conf;
        private final int pageSize;
        private boolean debug = false;

        public LogHandler(Map conf) {
            this.pageSize = ConfigExtension.getLogPageSize(conf);
            logDir = JStormUtils.getLogDir();
            String logDirPath = PathUtils.getCanonicalPath(logDir);
            if (logDirPath == null) {
                accessDirs.add(logDir);
            } else {
                accessDirs.add(logDirPath);
            }

            stormHome = System.getProperty("jstorm.home");
            if (stormHome != null) {
                String stormHomePath = PathUtils.getCanonicalPath(stormHome);
                if (stormHomePath == null) {
                    accessDirs.add(stormHome);
                } else {
                    accessDirs.add(stormHomePath);
                }
            }

            String confDir = System.getProperty(Constants.JSTORM_CONF_DIR);
            if (!StringUtils.isBlank(confDir)) {
                String confDirPath = PathUtils.getCanonicalPath(confDir);
                if (confDirPath != null) {
                    accessDirs.add(confDirPath);
                }
            }

            this.conf = conf;

            LOG.info("logview logDir=" + logDir); // +++
        }

        @VisibleForTesting
        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        @VisibleForTesting
        public void setLogDir(String dir) {
            this.logDir = dir;
        }

        public void handlFailure(HttpExchange t, String errorMsg) throws IOException {
            LOG.error(errorMsg);

            byte[] data = errorMsg.getBytes();
            sendResponse(t, HttpURLConnection.HTTP_BAD_REQUEST, data);
        }

        public void handle(HttpExchange t) throws IOException {
            URI uri = t.getRequestURI();
            Map<String, String> paramMap = parseRawQuery(uri.getRawQuery());
            LOG.info("Receive command " + paramMap);

            String cmd = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD);
            if (StringUtils.isBlank(cmd)) {
                handlFailure(t, "Bad Request, Not set command type");
                return;
            }

            if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW.equals(cmd)) {
                handleShowLog(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_LIST.equals(cmd)) {
                handleListDir(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_JSTACK.equals(cmd)) {
                handleJstack(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_JSTAT.equals(cmd)) {
                handleJstat(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW_CONF.equals(cmd)) {
                handleShowConf(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SEARCH_LOG.equals(cmd)) {
                handleSearchLog(t, paramMap);
            } else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_DOWNLOAD.equals(cmd)){
                handleDownloadLog(t, paramMap);
            } else {
                handlFailure(t, "Bad Request, Not support command type " + cmd);
            }
        }

        private void accessCheck(String fileName) throws IOException {
            if (debug) {
                return;
            }

            File file = new File(fileName);
            String canonicalPath = file.getCanonicalPath();

            boolean isChild = false;
            for (String dir : accessDirs) {
                if (canonicalPath.contains(dir)) {
                    isChild = true;
                    break;
                }
            }

            if (!isChild) {
                LOG.error("Access one disallowed path: " + canonicalPath);
                throw new IOException("Destination file/path is not accessible.");
            }
        }

        private Map<String, String> parseRawQuery(String uriRawQuery) {
            Map<String, String> paramMap = Maps.newHashMap();

            for (String param : StringUtils.split(uriRawQuery, "&")) {
                String[] pair = StringUtils.split(param, "=");
                if (pair.length == 2) {
                    paramMap.put(pair[0], pair[1]);
                }
            }

            return paramMap;
        }

        private void handleShowLog(HttpExchange t, Map<String, String> paramMap) throws IOException {
            Pair<Long, byte[]> logPair = queryLog(t, paramMap);
            if (logPair == null) {
                return;
            }

            String size = String.format(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_SIZE_FORMAT, logPair.getFirst());
            byte[] sizeByts = size.getBytes();

            byte[] logData = logPair.getSecond();

            t.sendResponseHeaders(HttpURLConnection.HTTP_OK, sizeByts.length + logData.length);
            OutputStream os = t.getResponseBody();
            os.write(sizeByts);
            os.write(logData);
            os.close();
        }

        private void handleDownloadLog(HttpExchange t, Map<String, String> paramMap) throws IOException {
            Pair<Long, byte[]> logPair = queryLog(t, paramMap);
            sendResponse(t, HttpURLConnection.HTTP_OK, logPair.getSecond());
        }


        private Pair<Long, byte[]> queryLog(HttpExchange t, Map<String, String> paramMap) throws IOException {

            String fileParam = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_LOGFILE);
            String _pageSize = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PAGE_SIZE);
            if (StringUtils.isBlank(fileParam)) {
                handlFailure(t, "Bad Request, Params Error, no log file name.");
                return null;
            }
            int pageSize = this.pageSize;
            if (!StringUtils.isBlank(_pageSize)) {
                pageSize = JStormUtils.parseInt(_pageSize, this.pageSize);
            }
            String logFile = Joiner.on(File.separator).join(logDir, fileParam);
            accessCheck(logFile);
            FileChannel fc = null;
            MappedByteBuffer fout;
            long fileSize;
            byte[] ret;
            try {
                fc = new RandomAccessFile(logFile, "r").getChannel();

                fileSize = fc.size();

                long position = fileSize - pageSize;
                try {
                    String posStr = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS);
                    if (!StringUtils.isBlank(posStr)) {
                        position = Long.valueOf(posStr);
                    }
                } catch (Exception e) {
                    LOG.warn("Invalide position " + position);
                }
                if (position < 0) {
                    position = 0L;
                }

                long size = Math.min(fileSize - position, pageSize);

                LOG.info("logview " + logFile + ", position=" + position + ", size=" + size);
                fout = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                ret = new byte[(int) size];
                fout.get(ret);
                String str = new String(ret, ConfigExtension.getLogViewEncoding(conf));
                return new Pair<>(fileSize, str.getBytes());

            } catch (FileNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                handlFailure(t, "Bad Request, Failed to find " + fileParam);
                return null;

            } catch (IOException e) {
                LOG.warn(e.getMessage(), e);
                handlFailure(t, "Bad Request, Failed to open " + fileParam);
                return null;
            } finally {
                if (fc != null) {
                    IOUtils.closeQuietly(fc);
                }
            }

        }

        byte[] getJSonFiles(String dir) throws Exception {
            Map<String, FileAttribute> fileMap = new HashMap<String, FileAttribute>();

            String path = logDir;
            if (dir != null) {
                path = path + File.separator + dir;
            }
            accessCheck(path);

            LOG.info("List dir " + path);

            File file = new File(path);

            String[] files = file.list();
            if (files == null) {
            	files = new String[] {};
            }

            for (String fileName : files) {
                String logFile = Joiner.on(File.separator).join(path, fileName);

                FileAttribute fileAttribute = new FileAttribute();
                fileAttribute.setFileName(fileName);

                File subFile = new File(logFile);

                Date modify = new Date(subFile.lastModified());
                fileAttribute.setModifyTime(TimeFormat.getSecond(modify));

                if (subFile.isFile()) {
                    fileAttribute.setIsDir(String.valueOf(false));
                    fileAttribute.setSize(String.valueOf(subFile.length()));

                    fileMap.put(logFile, fileAttribute);
                } else if (subFile.isDirectory()) {
                    fileAttribute.setIsDir(String.valueOf(true));
                    fileAttribute.setSize(String.valueOf(4096));

                    fileMap.put(logFile, fileAttribute);
                }

            }

            String fileJsonStr = JStormUtils.to_json(fileMap);
            return fileJsonStr.getBytes();
        }

        void handleListDir(HttpExchange t, Map<String, String> paramMap) throws IOException {
            byte[] filesJson;

            try {
                String dir = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_DIR);
                filesJson = getJSonFiles(dir);
            } catch (Exception e) {
                LOG.error("Failed to list files", e);
                handlFailure(t, "Failed to get file list");
                return;
            }

            sendResponse(t, HttpURLConnection.HTTP_OK, filesJson);
        }

        /**
         * log search, migrated from STORM-902
         *
         * the following params are needed:
         * - file *
         * - offset *
         * - max_match
         * - look_back: num of context lines backward from match
         * - look_ahead: num of context lines forward from match
         * - key:* search keyword
         * - case_ignore: true/false
         * - search_from: search direction, head: from begin to end, tail: from end to begin, default is tail
         *
         * following values are returned:
         * - error: true/false
         * - msg: the error msg if any
         * - num_match: the match num
         * - next_offset
         * - match_results: map<offset, match_result>  note that no consecutive match results are overlapped.
         */
        void handleSearchLog(HttpExchange t, Map<String, String> paramMap) throws IOException {
            int maxMatch = JStormUtils.parseInt(paramMap.get("max_match"), ConfigExtension.getMaxMatchPerLogSearch(conf));
            long offset = JStormUtils.parseLong(paramMap.get("offset"), 0);
            String logFile = paramMap.get("file");
            int lookBack = JStormUtils.parseInt(paramMap.get("look_back"), 2);
            int lookAhead = JStormUtils.parseInt(paramMap.get("look_ahead"), 10);
            boolean caseIgnore = JStormUtils.parseBoolean(paramMap.get("case_ignore"), false);
            int maxBlocks = ConfigExtension.getMaxBlocksPerLogSearch(conf);
            int blockSize = HttpserverUtils.LOG_SEARCH_BLOCK_SIZE;
            String key = paramMap.get("key");
            if (caseIgnore){
                key = key.toLowerCase();
            }
            key = URLDecoder.decode(key, "UTF-8");

            Map<Object, Object> ret = new HashMap<>();
            if (StringUtils.isBlank(key)) {
                error(ret, "search key cannot be empty!");

                String resp = JStormUtils.to_json(ret);
                sendResponse(t, HttpURLConnection.HTTP_OK, resp);
                return;
            }

            if (debug) {
                System.out.println("Search for key:" + key);
                System.out.println("===================================\n");
            }

            logFile = Joiner.on(File.separator).join(logDir, logFile);
            accessCheck(logFile);

            //search
            String searchFrom = paramMap.get("search_from");
            if (searchFrom != null && searchFrom.equals("head")){
                ret = searchFromHead(logFile, offset, key, maxMatch, lookBack, lookAhead, maxBlocks, blockSize, caseIgnore);
            } else {
                ret = searchFromTail(logFile, offset, key, maxMatch, lookBack, lookAhead, maxBlocks, blockSize, caseIgnore);
            }

            String resp = JStormUtils.to_json(ret);
            sendResponse(t, HttpURLConnection.HTTP_OK, resp);
        }


        private Map<Object, Object> searchFromTail(String logFile, long offset, String key, int maxMatch, int lookBack,
                           int lookAhead, int maxBlocks, int blockSize, boolean caseIgnore) throws IOException {
            Map<Object, Object> ret = new HashMap<>();
            Map<Long, String> matchResults = new HashMap<>();   //<offset, match content>
            int match = 0;

            String encoding = ConfigExtension.getLogViewEncoding(conf);
            FileChannel fc = null;
            MappedByteBuffer fout;
            final long fileSize;
            try {
                RandomAccessFile randomAccess = new RandomAccessFile(logFile, "r");
                fc = randomAccess.getChannel();
                fileSize = fc.size();

                if (offset == 0 || offset < fileSize) {
                    // if offset is 0, we search from the end.
                    long pos = fileSize;
                    // user have not specify search start position, we start from end default
                    if (offset > 0) {
                        pos = offset;
                    }
                    long matchOffset = pos;      // the offset of match result
                    int jumpLines = 0;
                    StringBuilder matchContent = new StringBuilder();

                    for (int i = 0; i < maxBlocks && pos > 0 && match < maxMatch; i++) {
                        long bufferSize = blockSize;
                        if (pos < blockSize) {
                            // if current pos is less than block size , we start search from begin
                            bufferSize = pos;
                            pos = 0;
                        } else {
                            pos = pos - bufferSize;
                        }
                        fout = fc.map(FileChannel.MapMode.READ_ONLY, pos, bufferSize);
                        byte[] buffer = new byte[(int) bufferSize];
                        fout.get(buffer);
                        String data = new String(buffer, encoding);
                        String[] lines = data.split("\\r\\n|\\n|\\r");

                        int[] line2pos = new int[lines.length];     //the position of the end of the line
                        for (int j = 0; j < lines.length; j++) {
                            line2pos[j] = lines[j].getBytes(encoding).length;
                            if (j > 0) {
                                line2pos[j] += line2pos[j - 1];
                            }
                        }

                        // begin to search this block
                        for (int j = 0; j < lines.length; ) {
                            String line = lines[j];
                            boolean isMatch = caseIgnore ? line.toLowerCase().contains(key) : line.contains(key);
                            if (isMatch) {
                                int start = Math.max(0, j - lookBack);
                                j += lookAhead;
                                matchOffset = start > 0 ? pos + line2pos[start-1] : pos;
                                // jumps out of current block
                                if (j >= lines.length) {
                                    jumpLines = j - lines.length + 1;
                                    j = lines.length - 1;
                                }
                                match++;

                                // make partial search result, if there's jumpLines, the result is not complete.
                                // will not append the last line
                                for (int k = start; k < j; k++) {
                                    matchContent.append(lines[k]).append("\n");
                                }
                                // concat the jump out lines
                                if (jumpLines > 0) {
                                    readJumpLines(randomAccess, pos + line2pos[j], jumpLines, matchContent);
                                    jumpLines = 0;
                                    j = lines.length;
                                }
                                // if it's the last match, we should ignore jumpLines
                                matchResults.put(matchOffset, matchContent.toString());
                                if (debug) {
                                    System.out.println("==== match " + match + " ==== offset: " + matchOffset);
                                    System.out.println(matchContent.toString());
                                    System.out.println();
                                }
                                matchContent = new StringBuilder();

                            } else {
                                j++;
                            }
                        }

                        if (match < maxMatch && pos > 0) {
                            // move back from the line/word break
                            int lineBreakOffset = lines[0].getBytes(encoding).length;
                            if (lineBreakOffset < blockSize) {
                                pos = pos + lineBreakOffset;
                            }
                        }
                    }
                    ret.put("num_match", match);
                    ret.put("match_results", matchResults);
                    ret.put("next_offset", pos);
                } else {
                    error(ret, "pos exceeds file size!");
                }
            } catch (FileNotFoundException e) {
                LOG.warn("Error", e);
                error(ret, "Bad Request, Failed to find " + logFile);
            } catch (IOException e) {
                LOG.warn("Error", e);
                error(ret, "Bad Request, Failed to open " + logFile);
            } finally {
                if (fc != null) {
                    IOUtils.closeQuietly(fc);
                }
            }

            return ret;
        }

        private void readJumpLines(RandomAccessFile randomAccessFile, long pos, int lines, StringBuilder matchContent){
            try {
                randomAccessFile.seek(pos);
                while(lines-- > 0){
                    String line = randomAccessFile.readLine();
                    if (line != null){
                        matchContent.append(line).append("\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Map<Object, Object> searchFromHead(String logFile, long offset, String key, int maxMatch, int lookBack,
                            int lookAhead, int maxBlocks, int blockSize, boolean caseIgnore) throws IOException {
            Map<Object, Object> ret = new HashMap<>();
            Map<Long, String> matchResults = new HashMap<>();   //<offset, match content>
            int match = 0;

            String encoding = ConfigExtension.getLogViewEncoding(conf);
            FileChannel fc = null;
            MappedByteBuffer fout;
            long fileSize;
            try {
                fc = new RandomAccessFile(logFile, "r").getChannel();
                fileSize = fc.size();

                if (offset < fileSize) {
                    long pos = offset;
                    long matchOffset = offset;
                    int jumpLines = 0;
                    StringBuilder matchContent = new StringBuilder();

                    for (int i = 0; i < maxBlocks && pos < fileSize && match < maxMatch; i++) {
                        long bufferSize = Math.min(fc.size() - pos, blockSize);
                        fout = fc.map(FileChannel.MapMode.READ_ONLY, pos, bufferSize);
                        byte[] buffer = new byte[(int) bufferSize];
                        fout.get(buffer);
                        String data = new String(buffer, encoding);
                        String[] lines = data.split("\\r\\n|\\n|\\r");

                        int[] line2pos = new int[lines.length];
                        for (int j = 0; j < lines.length; j++) {
                            line2pos[j] = lines[j].getBytes(encoding).length;
                            if (j > 0) {
                                line2pos[j] += line2pos[j - 1];
                            }
                        }

                        for (int j = 0; j < lines.length && match < maxMatch; ) {
                            // rotate lines from last match
                            if (jumpLines > 0) {
                                for (int m = 0; m < jumpLines && m < lines.length; m++) {
                                    matchContent.append(lines[m]).append("\n");
                                }
                                j = jumpLines;
                                jumpLines = 0;

//                                matchResults.add(matchContent.toString());
                                matchResults.put(matchOffset, matchContent.toString());
                                if (debug) {
//                                    System.out.println("==== match " + match + " ====");
                                    System.out.println("==== match " + match + " ==== offset: " + matchOffset);
                                    System.out.println(matchContent.toString());
                                    System.out.println();
                                }
                                matchContent = new StringBuilder();
                                continue;
                            }

                            String line = lines[j];
                            boolean isMatch = caseIgnore ? line.toLowerCase().contains(key) : line.contains(key);
                            if (isMatch) {
                                int start = Math.max(0, j - lookBack);
                                j += lookAhead;
                                matchOffset = start > 0 ? pos + line2pos[start-1] : pos;
                                // jumps out of current block
                                if (j >= lines.length) {
                                    jumpLines = j - lines.length + 1;
                                    j = lines.length -1 ;
                                }
                                // search finishes
                                if (++match >= maxMatch) {
                                    pos += line2pos[j];
                                }

                                // make partial search result, if there's jumpLines, the result is not complete.
                                for (int k = start; k < j; k++) {
                                    matchContent.append(lines[k]).append("\n");
                                }
                                // if it's the last match, we should ignore jumpLines
                                if (jumpLines == 0 || match >= maxMatch) {
//                                    matchResults.add(matchContent.toString());
                                    matchResults.put(matchOffset, matchContent.toString());
                                    if (debug) {
                                        System.out.println("==== match " + match + " ==== offset: " + matchOffset);
                                        System.out.println(matchContent.toString());
                                        System.out.println();
                                    }
                                    matchContent = new StringBuilder();
                                } else {
                                    j++;
                                }

                            } else {
                                j++;
                            }
                        }

                        if (match < maxMatch) {
                            // move back from the line/word break
                            int lineBreakOffset = lines[lines.length - 1].getBytes(encoding).length;
                            if (lineBreakOffset < blockSize) {
                                pos = pos + blockSize - lineBreakOffset;
                            } else {
                                pos += blockSize;
                            }
                        }
                    }
                    ret.put("num_match", match);
                    ret.put("match_results", matchResults);
                    ret.put("next_offset", pos);
                } else {
                    error(ret, "pos exceeds file size!");
                }
            } catch (FileNotFoundException e) {
                LOG.warn("Error", e);
                error(ret, "Bad Request, Failed to find " + logFile);
            } catch (IOException e) {
                LOG.warn("Error", e);
                error(ret, "Bad Request, Failed to open " + logFile);
            } finally {
                if (fc != null) {
                    IOUtils.closeQuietly(fc);
                }
            }

            return ret;
        }

        void error(Map<Object, Object> ret, String msg) {
            ret.put("error", true);
            ret.put("msg", msg);
        }

        void handleJstack(StringBuilder sb, Integer pid) {
            String cmd = "jstack " + pid;

            try {
                LOG.info("Begin to execute " + cmd);
                String output = JStormUtils.launchProcess(cmd, new HashMap<String, String>(), false);
                sb.append(output);
                
                LOG.info("Successfully get output of " + cmd);
            } catch (IOException e) {
                LOG.info("Failed to execute " + cmd, e);
                sb.append("Failed to execute " + cmd);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                sb.append("Failed to execute " + cmd + ", " + e.getCause());
            }
        }


        void handleJstat (StringBuilder sb, Integer pid) {
            String cmd = "jstat -gc " + pid;

            try {
                LOG.info("Begin to execute " + cmd);
                String output = JStormUtils.launchProcess(cmd, new HashMap<String, String>(), false);
                sb.append(output);

                LOG.info("Successfully get output of " + cmd);
            } catch (IOException e) {
                LOG.info("Failed to execute " + cmd, e);
                sb.append("Failed to execute " + cmd);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                sb.append("Failed to execute " + cmd + ", " + e.getCause());
            }
        }

        void handleJstack(HttpExchange t, Map<String, String> paramMap) throws IOException {
            String workerPort = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT);
            if (workerPort == null) {
                handlFailure(t, "Not set worker's port");
                return;
            }

            LOG.info("Begin to get jstack of " + workerPort);
            StringBuilder sb = new StringBuilder();
            List<Integer> pids = Worker.getOldPortPids(workerPort);
            for (Integer pid : pids) {
                sb.append("!!!!!!!!!!!!!!!!!!\r\n");
                sb.append("WorkerPort:" + workerPort + ", pid:" + pid);
                sb.append("\r\n!!!!!!!!!!!!!!!!!!\r\n");

                handleJstack(sb, pid);
            }

            byte[] data = sb.toString().getBytes();
            sendResponse(t, HttpURLConnection.HTTP_OK, data);
        }

        void handleJstat(HttpExchange t, Map<String, String> paramMap) throws IOException {
            String workerPort = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT);
            if (workerPort == null) {
                handlFailure(t, "Not set worker's port");
                return;
            }

            LOG.info("Begin to get jstat of " + workerPort);
            StringBuilder sb = new StringBuilder();
            List<Integer> pids = Worker.getOldPortPids(workerPort);
            for (Integer pid : pids) {
                sb.append("!!!!!!!!!!!!!!!!!!\r\n");
                sb.append("WorkerPort:" + workerPort + ", pid:" + pid);
                sb.append("\r\n!!!!!!!!!!!!!!!!!!\r\n");

                handleJstat(sb, pid);
            }

            byte[] data = sb.toString().getBytes();
            sendResponse(t, HttpURLConnection.HTTP_OK, data);
        }

        void handleShowConf(HttpExchange t, Map<String, String> paramMap) throws IOException {
            byte[] json;
            try {
                String tmp = Utils.to_json(conf);
                json = tmp.getBytes();
            } catch (Exception e) {
                LOG.error("Failed to get configuration", e);
                handlFailure(t, "Failed to get configuration");
                return;
            }

            sendResponse(t, HttpURLConnection.HTTP_OK, json);
        }

        void sendResponse(HttpExchange t, int retCode, String data) throws IOException {
            if (debug) {
                LOG.info("HTTP:{}, search result:{}", retCode, data);
            }
            byte[] bytes = data.getBytes();
            sendResponse(t, retCode, bytes);
        }

        void sendResponse(HttpExchange t, int retCode, byte[] data) throws IOException {
            if (t != null) {
                t.sendResponseHeaders(retCode, data.length);
                OutputStream os = t.getResponseBody();
                os.write(data);
                os.close();
            }
        }
    }// LogHandler

    public void start() {
        int numHandler = 3;
        InetSocketAddress socketAddr = new InetSocketAddress(port);
        Executor executor = Executors.newFixedThreadPool(numHandler);

        try {
            hs = HttpServer.create(socketAddr, 0);
            hs.createContext(HttpserverUtils.HTTPSERVER_CONTEXT_PATH_LOGVIEW, new LogHandler(conf));
            hs.setExecutor(executor);
            hs.start();

        } catch (BindException e) {
            LOG.info("HttpServer Already start!");
            hs = null;
            return;
        } catch (IOException e) {
            LOG.error("HttpServer Start Failed", e);
            hs = null;
            return;
        }
        LOG.info("Success start HttpServer at port:" + port);

    }

    @Override
    public void shutdown() {
        if (hs != null) {
            hs.stop(0);
            LOG.info("Successfully stop http server");
        }

    }

    public static void main(String[] args) throws Exception {
        Map conf = new HashMap();
        LogHandler logHandler = new LogHandler(conf);
        logHandler.setDebug(true);
        logHandler.setLogDir("/Users/wuchong/Downloads/");

        Map<String, String> params = new HashMap<>();
//        params.put("offset", "0");
//        params.put("search_from", "head");
        params.put("file", "SequenceTest6-worker-6800.log");
        logHandler.handleSearchLog(null, params);

        params.put("key", "info");
        logHandler.handleSearchLog(null, params);

        params.put("case_ignore", "true");
        logHandler.handleSearchLog(null, params);

        params.put("offset", "7481");
        logHandler.handleSearchLog(null, params);
    }

}
