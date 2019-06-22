package com.atypon.MapReduce.util;

import com.atypon.Globals;

import java.io.IOException;
import java.io.PrintWriter;

public class FileOutputWriter {
    private PrintWriter writer;

    public FileOutputWriter() {
        try {
            this.writer = new PrintWriter(Globals.OUTPUT_FILE_NAME, "UTF-8");
        } catch (IOException e) {
            System.out.println("Could not write to file");
        }
    }

    public void write(Object[] arr) {
        for (Object o : arr) {
            writer.println(o);
        }
    }

    public void close() {
        writer.close();
    }
}
