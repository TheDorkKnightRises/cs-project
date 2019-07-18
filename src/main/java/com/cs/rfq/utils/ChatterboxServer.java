package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

/**
 * Simple chat server capable of sending and receiving String lines on separate in/out port numbers.
 */
public class ChatterboxServer {

    public static final int SERVER_PORT_OUT = 9000;
    public static final int SERVER_PORT_IN = 9001;

    //thread for sending keyboard input to SERVER_PORT_OUT
    private static Thread rfqSenderOutputThread;

    //monitors SERVER_PORT_OUT for socket closing
    private static Thread rfqSenderInputThread;

    public static void main(String[] args) throws Exception {
        runSender();
        runReceiver();
    }

    private static void runSender() throws Exception {
        ServerSocket rfqServerSocket = new ServerSocket(SERVER_PORT_OUT);
        Thread main = new Thread(() -> {
            while (true) {
                try {
                    Thread.yield();
                    log("waiting to connect");
                    Socket socket = rfqServerSocket.accept();
                    log("connected");

                    rfqSenderOutputThread = send(socket);
                    rfqSenderInputThread = receive(socket);

                    rfqSenderOutputThread.start();
                    rfqSenderInputThread.start();

                    synchronized (rfqSenderInputThread) {
                        //will die when there is no testdata to read or the caller disconnects
                        rfqSenderInputThread.wait();
                    }

                    rfqSenderOutputThread.interrupt();
                    log("disconnected");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        main.setName("main");
        main.setDaemon(true);
        main.start();
    }

    private static void runReceiver() throws Exception {
        ServerSocket confServerSocket = new ServerSocket(SERVER_PORT_IN);
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = confServerSocket.accept();
                    receive(socket).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static Thread send(final Socket socket) {
        return new Thread(() -> {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
                List<String> historyEntries = ChatHistoryUtil.getHistoryEntries();
                String line = "";
                while (true) {
                    if (historyEntries.size() > 0) {
                        System.out.println("\nWhat would you like to do?\n" +
                                "1. Input RFQ\n" +
                                "2. Resend from history\n" +
                                "3. Exit");
                        switch (in.readLine().trim()) {
                            case "1":
                                line = in.readLine();
                                sendLine(line, in, out);
                                ChatHistoryUtil.addEntry(line);
                                break;
                            case "2":
                                historyEntries = ChatHistoryUtil.getHistoryEntries();
                                System.out.println("\nPick RFQ to resend");
                                for (int i = 0; i < historyEntries.size(); i++) {
                                    System.out.println((i + 1) + ": " + historyEntries.get(i));
                                }
                                int choice = Integer.parseInt(in.readLine());
                                if (choice <= historyEntries.size()) {
                                    line = historyEntries.get(choice - 1);
                                    sendLine(line, in, out);
                                } else {
                                    System.out.println("Invalid choice.");
                                }
                                break;
                            case "3":
                                System.out.println("Terminated");
                                return;
                            default:
                                System.out.println("Invalid choice. Please input options (1-3)");
                        }
                    } else {
                        line = in.readLine();
                        sendLine(line, in, out);
                        ChatHistoryUtil.addEntry(line);
                    }
                }
            } catch (IOException e) {
                log("Input output error");
            }
        });
    }

    static void sendLine(String line, BufferedReader in, PrintWriter out) {
        if (!"".equals(line.trim())) {
            try {
                new Gson().fromJson(line, Rfq.class);
                out.println(line);
                out.flush();
                log("sent", line);
            } catch (JsonSyntaxException e) {
                System.out.println("Invalid RFQ syntax");
            }
        }
    }

    private static Thread receive(final Socket socket) {
        return new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                String line = in.readLine();
                while (line != null) {
                    log("got response", line);
                    line = in.readLine();
                }
                socket.close();
                //System.out.println("---- close socket -----");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void log(String status) {
        log(status,"");
    }

    private static void log(String status, String message) {
        System.out.printf("%-10s> %-14s %s%n", Thread.currentThread().getName(), status, message);
    }
}
