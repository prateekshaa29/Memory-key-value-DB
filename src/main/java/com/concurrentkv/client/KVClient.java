package com.concurrentkv.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Reusable TCP Client for ConcurrentKVStore.
 * Provides programmatic API methods for all database operations
 * and implements AutoCloseable for try-with-resources.
 */
public class KVClient implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public KVClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    public synchronized void connect() throws IOException {
        this.socket = new Socket(host, port);
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
    }

    /**
     * Sends a raw command line to the server and returns the raw response.
     */
    public synchronized String sendRawCommand(String commandLine) throws IOException {
        if (socket == null || socket.isClosed()) {
            connect();
        }
        writer.write(commandLine + "\r\n");
        writer.flush();

        // Read response: if first line is multi-line or list or info, read appropriately
        String firstLine = reader.readLine();
        if (firstLine == null) {
            throw new EOFException("Connection closed by server");
        }

        if (firstLine.startsWith("# Server") || firstLine.startsWith("1)")) {
            // Multiline response
            StringBuilder sb = new StringBuilder();
            sb.append(firstLine).append("\n");
            // Check if there are more lines ready
            while (reader.ready()) {
                String next = reader.readLine();
                if (next == null) break;
                sb.append(next).append("\n");
            }
            return sb.toString().trim();
        }

        return firstLine;
    }

    public synchronized boolean set(String key, String value) throws IOException {
        return set(key, value, -1);
    }

    public synchronized boolean set(String key, String value, long ttlSeconds) throws IOException {
        String quotedVal = escapeAndQuote(value);
        String cmd = (ttlSeconds > 0)
                ? String.format("SET %s %s %d", quoteIfSpaced(key), quotedVal, ttlSeconds)
                : String.format("SET %s %s", quoteIfSpaced(key), quotedVal);

        String resp = sendRawCommand(cmd);
        return resp.startsWith("OK");
    }

    public synchronized String get(String key) throws IOException {
        String resp = sendRawCommand("GET " + quoteIfSpaced(key));
        if (resp.equals("(nil)")) {
            return null;
        }
        if (resp.startsWith("VALUE ")) {
            return resp.substring("VALUE ".length());
        }
        if (resp.startsWith("ERR")) {
            throw new IOException(resp);
        }
        return resp;
    }

    public synchronized boolean del(String key) throws IOException {
        String resp = sendRawCommand("DEL " + quoteIfSpaced(key));
        return resp.equals("(integer) 1");
    }

    public synchronized boolean ttl(String key, long seconds) throws IOException {
        String resp = sendRawCommand(String.format("TTL %s %d", quoteIfSpaced(key), seconds));
        return resp.equals("(integer) 1");
    }

    public synchronized long pttl(String key) throws IOException {
        String resp = sendRawCommand("PTTL " + quoteIfSpaced(key));
        if (resp.startsWith("(integer) ")) {
            return Long.parseLong(resp.substring("(integer) ".length()).trim());
        }
        throw new IOException("Unexpected response: " + resp);
    }

    public synchronized boolean exists(String key) throws IOException {
        String resp = sendRawCommand("EXISTS " + quoteIfSpaced(key));
        return resp.equals("(integer) 1");
    }

    public synchronized List<String> keys() throws IOException {
        String resp = sendRawCommand("KEYS");
        List<String> keyList = new ArrayList<>();
        if (resp.equals("(empty list or set)")) {
            return keyList;
        }

        String[] lines = resp.split("\n");
        for (String line : lines) {
            line = line.trim();
            int quoteStart = line.indexOf('"');
            int quoteEnd = line.lastIndexOf('"');
            if (quoteStart != -1 && quoteEnd > quoteStart) {
                keyList.add(line.substring(quoteStart + 1, quoteEnd));
            } else if (line.contains(")")) {
                keyList.add(line.substring(line.indexOf(')') + 1).trim());
            }
        }
        return keyList;
    }

    public synchronized int dbsize() throws IOException {
        String resp = sendRawCommand("DBSIZE");
        if (resp.startsWith("(integer) ")) {
            return Integer.parseInt(resp.substring("(integer) ".length()).trim());
        }
        throw new IOException("Unexpected response: " + resp);
    }

    public synchronized boolean flushall() throws IOException {
        String resp = sendRawCommand("FLUSHALL");
        return resp.startsWith("OK");
    }

    public synchronized String ping() throws IOException {
        return sendRawCommand("PING");
    }

    public synchronized String ping(String message) throws IOException {
        return sendRawCommand("PING " + quoteIfSpaced(message));
    }

    public synchronized String save() throws IOException {
        return sendRawCommand("SAVE");
    }

    public synchronized String info() throws IOException {
        return sendRawCommand("INFO");
    }

    private String quoteIfSpaced(String str) {
        if (str == null) return "\"\"";
        if (str.contains(" ") || str.contains("\t") || str.contains("\"")) {
            return escapeAndQuote(str);
        }
        return str;
    }

    private String escapeAndQuote(String str) {
        if (str == null) return "\"\"";
        String escaped = str.replace("\\", "\\\\")
                            .replace("\"", "\\\"")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r")
                            .replace("\t", "\\t");
        return "\"" + escaped + "\"";
    }

    @Override
    public synchronized void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            try {
                sendRawCommand("QUIT");
            } catch (Exception ignored) {}
            socket.close();
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
}
