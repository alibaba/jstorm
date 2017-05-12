package backtype.storm.command;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import java.security.InvalidParameterException;
import java.util.Map;

/**
 * @author fengjian
 * @since 2.1.1
 */
public class blacklist {

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            throw new InvalidParameterException("Please input action and hostname");
        }

        String action = args[0];
        String hostname = args[1];

        NimbusClient client = null;
        try {

            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);

            if (action.equals("add"))
                client.getClient().setHostInBlackList(hostname);
            else {
                client.getClient().removeHostOutBlackList(hostname);
            }

            System.out.println("Successfully submit command blacklist with action:" + action + " and hostname :" + hostname);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
