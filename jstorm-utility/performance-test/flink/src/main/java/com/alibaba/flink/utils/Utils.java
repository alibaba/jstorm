package com.alibaba.flink.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;

import java.util.LinkedList;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class Utils {

    public static Object from_json(String json) {
        if (json == null) {
            return null;
        } else {
            // return JSON.parse(json);
            return JSON.parse(json);
        }
    }

    /**
     * seconds to string like '30m 40s' and '1d 20h 30m 40s'
     *
     * @param secs
     * @return
     */
    public static String prettyUptime(int secs) {
        String[][] PRETTYSECDIVIDERS = { new String[] {"ms", "1000"}, new String[] { "s", "60" }, new String[] { "m", "60" }, new String[] { "h", "24" },
                new String[] { "d", null } };
        int diversize = PRETTYSECDIVIDERS.length;

        LinkedList<String> tmp = new LinkedList<>();
        int div = secs;
        for (int i = 0; i < diversize; i++) {
            if (PRETTYSECDIVIDERS[i][1] != null) {
                Integer d = Integer.parseInt(PRETTYSECDIVIDERS[i][1]);
                tmp.addFirst(div % d + PRETTYSECDIVIDERS[i][0]);
                div = div / d;
            } else {
                tmp.addFirst(div + PRETTYSECDIVIDERS[i][0]);
            }
            if (div <= 0 ) break;
        }

        Joiner joiner = Joiner.on(" ");
        return joiner.join(tmp);
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }

        if (o instanceof Integer ||
                o instanceof Short ||
                o instanceof Byte) {
            return ((Number) o).intValue();
        } else if (o instanceof Long) {
            final long l = (Long) o;
            if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                return (int) l;
            }
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }

        throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
    }

    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    public static Long parseLong(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf(value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
        }
    }

    public static Long parseLong(Object o, long defaultValue) {

        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf((Integer) value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            return defaultValue;
        }
    }


}
