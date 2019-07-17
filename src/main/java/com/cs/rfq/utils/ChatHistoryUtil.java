package com.cs.rfq.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ChatHistoryUtil {

    public static List<String> getHistoryEntries() {
        List<String> list = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("chat-history.txt"));
            String tmp;
            while ((tmp = reader.readLine()) != null)
                list.add(tmp);
            reader.close();
            return list;
        } catch (IOException e) {
            return list;
        }
    }

    public static void addEntry(String line) {
        List<String> list = getHistoryEntries();
        list.add(0, line);
        if (list.size() > 5) {
            list = list.subList(0, 5);
        }
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("chat-history.txt"));
            for (String s : list)
                writer.write(s + "\r\n");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Error writing to history");
        }
    }

}
