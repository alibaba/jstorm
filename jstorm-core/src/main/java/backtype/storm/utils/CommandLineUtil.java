package backtype.storm.utils;

/**
 * @author wange
 * @since 23/01/2017
 */
public class CommandLineUtil {
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void error(String msg) {
        System.err.println(ANSI_RED + msg + ANSI_RESET);
    }

    public static void success(String msg) {
        System.out.println(ANSI_GREEN + msg + ANSI_RESET);
    }
}
