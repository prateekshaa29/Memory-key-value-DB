package com.concurrentkv.protocol;

import java.util.List;
import java.util.Map;

/**
 * Standard text protocol response formatters for ConcurrentKVStore.
 * All responses terminate with CRLF (\r\n).
 */
public class Response {

    public static final String CRLF = "\r\n";
    public static final String OK = "OK" + CRLF;
    public static final String NIL = "(nil)" + CRLF;
    public static final String PONG = "PONG" + CRLF;
    public static final String EMPTY_LIST = "(empty list or set)" + CRLF;

    public static String ok() {
        return OK;
    }

    public static String ok(String message) {
        return "OK " + message + CRLF;
    }

    public static String value(String val) {
        if (val == null) {
            return NIL;
        }
        return "VALUE " + val + CRLF;
    }

    public static String integer(long number) {
        return "(integer) " + number + CRLF;
    }

    public static String nil() {
        return NIL;
    }

    public static String error(String message) {
        return "ERR " + message + CRLF;
    }

    public static String pong() {
        return PONG;
    }

    public static String pong(String message) {
        return message + CRLF;
    }

    public static String keys(List<String> keyList) {
        if (keyList == null || keyList.isEmpty()) {
            return EMPTY_LIST;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyList.size(); i++) {
            sb.append(i + 1).append(") \"").append(keyList.get(i)).append("\"").append(CRLF);
        }
        return sb.toString();
    }

    public static String info(Map<String, Object> stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Server & Storage Info").append(CRLF);
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(CRLF);
        }
        return sb.toString();
    }
}
