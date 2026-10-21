package com.concurrentkv.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Protocol and Command Parser Tests")
public class ProtocolTest {

    private final CommandParser parser = new CommandParser(1024);

    @Test
    @DisplayName("Parses basic SET and GET commands")
    void testBasicCommands() {
        Command cmd = parser.parse("SET mykey myval");
        assertNotNull(cmd);
        assertEquals(CommandType.SET, cmd.getType());
        assertEquals("mykey", cmd.getArg(0));
        assertEquals("myval", cmd.getArg(1));

        Command getCmd = parser.parse("GET mykey");
        assertNotNull(getCmd);
        assertEquals(CommandType.GET, getCmd.getType());
        assertEquals("mykey", getCmd.getArg(0));
    }

    @Test
    @DisplayName("Handles quoted strings with spaces and escapes")
    void testQuotedStrings() {
        Command cmd = parser.parse("SET message \"Hello World from ConcurrentKV!\"");
        assertNotNull(cmd);
        assertEquals("message", cmd.getArg(0));
        assertEquals("Hello World from ConcurrentKV!", cmd.getArg(1));

        // Single quotes
        Command singleCmd = parser.parse("SET title 'Senior Software Engineer'");
        assertEquals("Senior Software Engineer", singleCmd.getArg(1));

        // Escaped quotes
        Command escCmd = parser.parse("SET quote \"He said \\\"Hello!\\\"\"");
        assertEquals("He said \"Hello!\"", escCmd.getArg(1));
    }

    @Test
    @DisplayName("Parses SET with optional TTL")
    void testSetWithTtl() {
        Command cmd = parser.parse("SET sessionToken abc123xyz 3600");
        assertNotNull(cmd);
        assertEquals(3, cmd.getArgCount());
        assertEquals("sessionToken", cmd.getArg(0));
        assertEquals("abc123xyz", cmd.getArg(1));
        assertEquals("3600", cmd.getArg(2));
    }

    @Test
    @DisplayName("Rejects wrong number of arguments")
    void testArgumentValidation() {
        // SET requires at least 2 args
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, 
                () -> parser.parse("SET onlykey"));
        assertTrue(ex1.getMessage().contains("wrong number of arguments"));

        // GET requires 1 arg
        assertThrows(IllegalArgumentException.class, () -> parser.parse("GET k1 k2"));

        // TTL requires valid integer
        assertThrows(IllegalArgumentException.class, () -> parser.parse("TTL key not_a_number"));

        // Unknown command
        assertThrows(IllegalArgumentException.class, () -> parser.parse("UNKNOWNCMD foo bar"));
    }

    @Test
    @DisplayName("Detects unclosed quotation marks")
    void testUnclosedQuote() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse("SET k \"unclosed string"));
    }

    @Test
    @DisplayName("Rejects requests exceeding maxLineLength")
    void testOversizedRequest() {
        CommandParser smallParser = new CommandParser(50);
        String longLine = "SET key " + "A".repeat(100);
        assertThrows(IllegalArgumentException.class, () -> smallParser.parse(longLine));
    }

    @Test
    @DisplayName("Response formatters adhere to protocol standards")
    void testResponseFormats() {
        assertEquals("OK\r\n", Response.ok());
        assertEquals("VALUE test\r\n", Response.value("test"));
        assertEquals("(nil)\r\n", Response.nil());
        assertEquals("(integer) 42\r\n", Response.integer(42));
        assertEquals("ERR something bad\r\n", Response.error("something bad"));
        assertEquals("PONG\r\n", Response.pong());
    }
}
