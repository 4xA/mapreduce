package com.atypon.Base.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * {@link InputReader} is a utility class
 * responsible for reading from input file.
 * @author  Asa Abbad
 */
public class InputReader {
    private InputReader() {}

    /**
     * Read lines from input file.
     * @param fileName  Name of input file
     * @return  {@link ArrayList} representing file
     */
    public static ArrayList<String> readLinesFromFile(String fileName) {
        ArrayList<String> list = new ArrayList<String>();

        String line = null;

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null)
                list.add(line.replace("\uFEFF", ""));

            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println( "Unable to open file '" + fileName + "'");
        }
        catch(IOException ex) {
            System.out.println( "Error reading file '" + fileName + "'");
        }

        return list;
    }
}
