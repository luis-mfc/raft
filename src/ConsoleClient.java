import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class ConsoleClient {
    private static final int MAX_ATTEMPTS = 10;
    private static Socket socket;
    private static ObjectInputStream inStream;
    private static ObjectOutputStream outStream;
    private static int clientID;
    private static int serverPort;
    private static int serverNumber;
    private static List<String> servers;
    private static String[] args;

    static public void main(String args[]) throws Exception {
        socket = null;
        inStream = null;
        outStream = null;
        clientID = -1;
        serverPort = -1;
        serverNumber = -1;
        servers = new ArrayList<>();
        ConsoleClient.args = args;
        if (args.length > 1) {
            System.err.println("Usage: ConsoleClient [serverNumber]");
            return;
        }

        try (InputStream input = new FileInputStream("./config.conf")) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
            while (bufferedReader.ready())
                servers.add(bufferedReader.readLine());
            if (args.length == 1) {
                serverNumber = Integer.parseInt(args[0]);
            } else {
                serverNumber = new Random().nextInt(servers.size());
            }
            serverPort = Integer.parseInt(servers.get(serverNumber));
        } catch (IndexOutOfBoundsException | IOException e) {
            System.err.println("Error starting Client");
            return;
        }

        if (!connect())
            return;

        Scanner sc = new Scanner(System.in);
        String line;

        System.out.println("Menu:");
        System.out.println("put <key> <value>");
        System.out.println("get <key>");
        System.out.println("del <key>");
        System.out.println("cas <key> <prevValue> <newValue>");
        System.out.println("list");
        System.out.println("help");
        System.out.println("exit\n");
        System.out.print("Insert a command:\n>>> ");

        // if exceptionInLastCommand then try again
        String[] options;
        String operation;
        String result;
        while (true) {
            line = sc.nextLine().toLowerCase();
            options = line.split(" ");
            operation = options[0];

            boolean completed = false;
            while (!completed) {
                try {
                    switch (operation) {
                    case "p":
                    case "put":
                        if (options.length != 3) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        outStream.writeObject(new Command(Command.OP_TYPE.PUT, options[1], options[2], null));
                        result = (String) inStream.readObject();
                        if (result.equals("-1"))
                            System.err.println("The server returned with an error, the operation was unsuccessful");
                        break;
                    case "g":
                    case "get":
                        if (options.length != 2) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        outStream.writeObject(new Command(Command.OP_TYPE.GET, options[1], null, null));
                        result = (String) inStream.readObject();
                        if (result.equals("-1"))
                            System.err.println("The server returned with an error, the operation was unsuccessful");
                        else
                            System.out.println(result);
                        break;
                    case "d":
                    case "del":
                        if (options.length != 2) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        outStream.writeObject(new Command(Command.OP_TYPE.DEL, options[1], null, null));
                        result = (String) inStream.readObject();
                        if (result.equals("-1"))
                            System.err.println("The server returned with an error, the operation was unsuccessful");
                        break;
                    case "l":
                    case "list":
                        if (options.length != 1) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        outStream.writeObject(new Command(Command.OP_TYPE.LIST, null, null, null));
                        result = (String) inStream.readObject();
                        if (result.equals("-1"))
                            System.err.println("The server returned with an error, the operation was unsuccessful");
                        else
                            System.out.println(result);
                        break;
                    case "c":
                    case "cas":
                        if (options.length != 4) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        outStream.writeObject(new Command(Command.OP_TYPE.CAS, options[1], options[3], options[2]));
                        result = (String) inStream.readObject();
                        if (result.equals("-1"))
                            System.err.println("The server returned with an error, the operation was unsuccessful");
                        break;
                    case "h":
                    case "help":
                        if (options.length != 1) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        System.out.println("Menu:");
                        System.out.println("put <key> <value>");
                        System.out.println("get <key>");
                        System.out.println("del <key>");
                        System.out.println("cas <key> <prevValue> <newValue>");
                        System.out.println("list");
                        System.out.println("help");
                        System.out.println("exit\n");
                        break;
                    case "e":
                    case "exit":
                        if (options.length != 1) {
                            System.err.println("Unknown command. Try again.");
                            break;
                        }
                        sc.close();
                        return;
                    default:
                        System.err.println("Unknown command. Try again.");
                        break;
                    }
                    completed = true;
                } catch (Exception e) {
                    if (!connect())
                        return;
                    continue;
                }
            }
            System.out.print(">>> ");
        }
    }

    private static boolean connect() {
        String hostName = "localhost";
        // establish a TCP connection
        if (serverPort == -1) {
            System.err.println("Error starting Client");
            return false;
        }
        int attempt = 0;
        int id = -1;
        while (id == -1 && attempt < MAX_ATTEMPTS) {
            try {
                socket = new Socket(hostName, serverPort);

                inStream = new ObjectInputStream(socket.getInputStream());
                outStream = new ObjectOutputStream(socket.getOutputStream());

                outStream.writeObject(clientID);

                String serverOutput = ((String) inStream.readObject());
                // Check if the leader was the server that was contacted
                if (serverOutput.contains(":")) {
                    serverPort = Integer.valueOf(serverOutput.substring(serverOutput.indexOf(":") + 1));
                    socket.close();
                } else
                    id = Integer.parseInt(serverOutput);
                attempt++;
                if (id == -1)
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e1) {
                    }
            } catch (IOException | ClassNotFoundException e) {
                if (args.length == 0) {
                    serverNumber = new Random().nextInt(servers.size());
                    serverPort = Integer.parseInt(servers.get(serverNumber));
                }
                attempt++;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
            }
        }
        if (attempt == MAX_ATTEMPTS && clientID == -1) {
            System.err.println("Error connecting to the server");
            return false;
        }
        clientID = id;
        return true;
    }
}
