package com.atypon.Base.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link Splitter} is a utility class
 * responsible for handling input by applying
 * a <b>regex pattern</b> or <b>splitting tokens</b>.
 * @author Asa Abbad
 */
public class Splitter {
    public static final String WORD = "word";

    private Splitter() {}

    /**
     * Split input using tokens.
     * @param string    Input {@link String}
     * @param splitters Splitter tokens
     * @return  {@link ArrayList} containing split input
     */
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

    /**
     * Split input using regex.
     * @param string    Input {@link String}
     * @param regex Regex pattern
     * @return  {@link ArrayList} containing split input
     */
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
