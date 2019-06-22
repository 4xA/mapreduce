package com.atypon.Base.util;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * {@link FileOutputWriter} is responsible for
 * handling file IO operations.
 * @author Asa Abbad
 */
public class FileOutputWriter {
    private PrintWriter writer;

    /**
     * Instantiate {@link FileOutputWriter}.
     * @param fileName  name of output file
     */
    public FileOutputWriter(String fileName) {
        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (IOException e) {
            System.out.println("Could not write to file");
        }
    }

    /**
     * Write an array of {@link Object} to file.
     * @param arr   Array to be written to file
     */
    public void write(Object[] arr) {
        for (Object o : arr) {
            writer.println(o);
        }
    }

    /**
     * Write a single line to file
     * @param s {@link String} line to be written to file
     */
    public void write(String s) {
        writer.println(s);
    }

    /**
     * Release resources held be {@link FileOutputWriter}
     */
    public void close() {
        writer.close();
    }
}
