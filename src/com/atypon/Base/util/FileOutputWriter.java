package com.atypon.Base.util;

import java.io.IOException;
import java.io.PrintWriter;

public class FileOutputWriter {
    private PrintWriter writer;

    public FileOutputWriter(String fileName) {
        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (IOException e) {
            System.out.println("Could not write to file");
        }
    }

    public void write(Object[] arr) {
        for (Object o : arr) {
            writer.println(o);
        }
    }

    public void write(String s) {
        writer.println(s);
    }

    public void close() {
        writer.close();
    }
}
