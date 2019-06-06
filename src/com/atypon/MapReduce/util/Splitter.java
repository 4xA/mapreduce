package com.atypon.MapReduce.util;

import java.util.ArrayList;
import java.util.Arrays;

public class Splitter {
    private Splitter() {}

    public static ArrayList<String> split(String string, String ...splitters) {
        if (splitters.length == 0) return null;

        String regex = "";

        // Make splitters valid regex
        for (int i = 0; i < splitters.length; i++) {
            if (splitters[i].contains("\\"))splitters[i] =  splitters[i].replace("\\", "\\\\");
            if (splitters[i].contains(".")) splitters[i] = splitters[i].replace(".", "\\.");
            if (splitters[i].contains("^")) splitters[i] = splitters[i].replace("^", "\\^");
            if (splitters[i].contains("$")) splitters[i] = splitters[i].replace("$", "\\$");
            if (splitters[i].contains("*")) splitters[i] = splitters[i].replace("*", "\\*");
            if (splitters[i].contains("+")) splitters[i] = splitters[i].replace("+", "\\+");
            if (splitters[i].contains("?")) splitters[i] = splitters[i].replace("?", "\\?");
            if (splitters[i].contains("(")) splitters[i] = splitters[i].replace("(", "\\(");
            if (splitters[i].contains(")")) splitters[i] = splitters[i].replace(")", "\\)");
            if (splitters[i].contains("[")) splitters[i] = splitters[i].replace("[", "\\[");
            if (splitters[i].contains("]")) splitters[i] = splitters[i].replace("]", "\\]");
            if (splitters[i].contains("|")) splitters[i] = splitters[i].replace("|", "\\|");
            if (splitters[i].contains("-")) splitters[i] = splitters[i].replace("-", "\\-");

            if (i < splitters.length-1)
                regex += splitters[i] + "|";
        }
        regex += splitters[splitters.length-1];

        String[] array = string.split(regex);

        return new ArrayList<String>(Arrays.asList(array));
    }
}
