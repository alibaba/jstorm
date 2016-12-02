package com.alibaba.flink.utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class MetricsMonitor {

    public static String ip;
    public static int port = 8081;
    public static String jobId;

    public static Map httpResponse(String url) {
        Map ret;
        try {
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpGet getRequest = new HttpGet(url);
            getRequest.addHeader("accept", "application/json");

            HttpResponse response = httpClient.execute(getRequest);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatusLine().getStatusCode());
            }

            String data = EntityUtils.toString(response.getEntity());

            ret = (Map) Utils.from_json(data);

            httpClient.getConnectionManager().shutdown();

        } catch (IOException e) {
            ret = errorMsg(e.getMessage());
        }
        return ret;
    }

    public static Map errorMsg(String msg) {
        Map<String, String> error = new HashMap<>();
        error.put("error", msg);
        return error;
    }


    public static String monitorJob() {
        StringBuilder sb = new StringBuilder();
        String url = String.format("http://%s:%d/jobs/%s", ip, port, jobId);
        Map json = httpResponse(url);
        String jobName = (String) json.get("name");
        String uptime = Utils.prettyUptime(Utils.getInt(json.get("duration"), 0));
        sb.append("job: ").append(jobName).append(" uptime: ").append(uptime).append("\n");
        List<Map> vertices = (List<Map>) json.get("vertices");
        for (Map v : vertices) {
            String name = (String) v.get("name");
            int duration = (int) v.get("duration");
            Map metrics = (Map) v.get("metrics");
            long recv = Utils.parseLong(metrics.get("read-records"));
            long send = Utils.parseLong(metrics.get("write-records"));
            double recvTps = (recv * 1000.0) / duration;
            double sendTps = (send * 1000.0) / duration;
            sb.append(String.format("[<%s> recv_tps: %.2f, send_tps: %.2f]\n", name, recvTps, sendTps));
        }
        sb.append("---------------------------------------------\n");
        return sb.toString();
    }


    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.out.println("[USAGE] MetricsMonitor <jobmanager-ip> [port] <jobid>");
            return;
        }

        ip = args[0];

        if (args.length == 3) {
            port = Utils.getInt(args[1], 8081);
            jobId = args[2];
        } else {
            jobId = args[1];
        }

        while (true) {
            System.out.println(monitorJob());
            Thread.sleep(1000 * 10);
        }
    }

}
