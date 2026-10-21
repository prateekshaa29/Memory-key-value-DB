package com.concurrentkv.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Line-based command parser for ConcurrentKVStore.
 * Supports tokenization, double-quoted and single-quoted strings with escape sequences,
 * and command argument validation.
 */
public class CommandParser {

    private final int maxLineLength;

    public CommandParser(int maxLineLength) {
        this.maxLineLength = maxLineLength > 0 ? maxLineLength : 65536;
    }

    public CommandParser() {
        this(65536);
    }

    /**
     * Parses a raw line of text received from a TCP client.
     *
     * @param line raw text line
     * @return Command object or null if empty
     * @throws IllegalArgumentException if syntax is invalid or limits exceeded
     */
    public Command parse(String line) {
        if (line == null) {
            return null;
        }

        if (line.length() > maxLineLength) {
            throw new IllegalArgumentException("Request exceeds maximum line length of " + maxLineLength + " characters");
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        List<String> tokens = tokenize(trimmed);
        if (tokens.isEmpty()) {
            return null;
        }

        String commandWord = tokens.get(0);
        CommandType type = CommandType.fromString(commandWord);
        if (type == null) {
            throw new IllegalArgumentException("Unknown command '" + commandWord + "'");
        }

        List<String> args = tokens.subList(1, tokens.size());
        validateArguments(type, args);

        return new Command(type, args);
    }

    /**
     * Tokenizes a command line respecting single and double quotes.
     */
    public List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        boolean escapeNext = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (escapeNext) {
                switch (c) {
                    case 'n': currentToken.append('\n'); break;
                    case 'r': currentToken.append('\r'); break;
                    case 't': currentToken.append('\t'); break;
                    case '"': currentToken.append('"'); break;
                    case '\'': currentToken.append('\''); break;
                    case '\\': currentToken.append('\\'); break;
                    default: currentToken.append(c); break;
                }
                escapeNext = false;
                continue;
            }

            if (c == '\\') {
                if (inDoubleQuote || inSingleQuote) {
                    escapeNext = true;
                } else {
                    currentToken.append('\\');
                }
                continue;
            }

            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (Character.isWhitespace(c) && !inDoubleQuote && !inSingleQuote) {
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken.setLength(0);
                }
            } else {
                currentToken.append(c);
            }
        }

        if (inDoubleQuote || inSingleQuote) {
            throw new IllegalArgumentException("Unclosed quotation mark in command");
        }

        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        return tokens;
    }

    /**
     * Validates that the number and format of arguments match the command specification.
     */
    private void validateArguments(CommandType type, List<String> args) {
        switch (type) {
            case SET:
                if (args.size() < 2 || args.size() > 3) {
                    throw new IllegalArgumentException("wrong number of arguments for 'set' command (expected: SET key value [ttl_seconds])");
                }
                if (args.size() == 3) {
                    try {
                        long ttl = Long.parseLong(args.get(2));
                        if (ttl < 0) {
                            throw new IllegalArgumentException("TTL must be non-negative");
                        }
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("value is not an integer or out of range");
                    }
                }
                break;

            case GET:
                if (args.size() != 1) {
                    throw new IllegalArgumentException("wrong number of arguments for 'get' command (expected: GET key)");
                }
                break;

            case DEL:
                if (args.isEmpty()) {
                    throw new IllegalArgumentException("wrong number of arguments for 'del' command (expected: DEL key)");
                }
                break;

            case TTL:
                if (args.size() != 2) {
                    throw new IllegalArgumentException("wrong number of arguments for 'ttl' command (expected: TTL key seconds)");
                }
                try {
                    Long.parseLong(args.get(1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("value is not an integer or out of range");
                }
                break;

            case PTTL:
                if (args.size() != 1) {
                    throw new IllegalArgumentException("wrong number of arguments for 'pttl' command (expected: PTTL key)");
                }
                break;

            case EXISTS:
                if (args.size() != 1) {
                    throw new IllegalArgumentException("wrong number of arguments for 'exists' command (expected: EXISTS key)");
                }
                break;

            case KEYS:
            case DBSIZE:
            case FLUSHALL:
            case SAVE:
            case INFO:
            case QUIT:
                if (!args.isEmpty()) {
                    throw new IllegalArgumentException("wrong number of arguments for '" + type.name().toLowerCase() + "' command");
                }
                break;

            case PING:
                if (args.size() > 1) {
                    throw new IllegalArgumentException("wrong number of arguments for 'ping' command");
                }
                break;
        }
    }
}
