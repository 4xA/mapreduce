package com.atypon.Map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Mapper {
    public static void main(String[] args) {
        System.out.println("Mapper process started");
        System.out.println("Input recieved: ");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        try {
            String s;
            while ((s = reader.readLine()) != null)
                System.out.println(s);

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
