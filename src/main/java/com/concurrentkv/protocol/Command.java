package com.concurrentkv.protocol;

import java.util.Collections;
import java.util.List;

/**
 * Represents a parsed client command.
 */
public class Command {

    private final CommandType type;
    private final List<String> args;

    public Command(CommandType type, List<String> args) {
        this.type = type;
        this.args = args != null ? Collections.unmodifiableList(args) : Collections.emptyList();
    }

    public CommandType getType() {
        return type;
    }

    public List<String> getArgs() {
        return args;
    }

    public int getArgCount() {
        return args.size();
    }

    public String getArg(int index) {
        if (index < 0 || index >= args.size()) {
            return null;
        }
        return args.get(index);
    }

    @Override
    public String toString() {
        return "Command{" +
                "type=" + type +
                ", args=" + args +
                '}';
    }
}
