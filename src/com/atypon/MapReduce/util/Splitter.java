package com.atypon.MapReduce.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Splitter {
    public static final String WORD = "word";

    private Splitter() {}

    public static ArrayList<String> split(String string, String ...splitters) {
        if (splitters.length == 0) return null;

        String regex = "";
        String[] array;

        // Make splitters valid regex
        for (int i = 0; i < splitters.length; i++) {
            if (splitters[i].contains("\\")) splitters[i] = splitters[i].replace("\\", "\\\\");
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

            if (i < splitters.length - 1)
                regex += splitters[i] + "|";
        }
        regex += splitters[splitters.length - 1];

        array = string.split(regex);

        return new ArrayList<String>(Arrays.asList(array));
    }

    public static ArrayList<String> split(String string, String regex) {
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(string);

            ArrayList<String> list = new ArrayList<>();

            while(matcher.find()) {
                String word = matcher.group();
                list.add(word);
            }

            return list;
    }
}
