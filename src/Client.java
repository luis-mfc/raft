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

public class Client {
	private static final int MAX_ATTEMPTS = 10;
	private static int clientID;
	private static int serverPort;
	private static int serverNumber;
	private static Socket socket;
	private static ObjectInputStream inStream;
	private static ObjectOutputStream outStream;
	private static List<String> servers;
	private static String[] args;

	static public void main(String args[]) throws Exception {
		clientID = -1;
		serverPort = -1;
		serverNumber = -1;
		socket = null;
		inStream = null;
		outStream = null;
		servers = new ArrayList<>();
		Client.args = args;

		if (args.length == 0 || args.length > 3) {
			System.err.println("Usage: Client [serverNumber] <put|get|del|cas|list> <numberOfRequests>");
			return;
		}
		int numberOfRequests;
		try {
			if ((numberOfRequests = Integer.valueOf(args[args.length - 1])) < 1) {
				System.err.println("Usage: Client [serverNumber] <put|get|del|cas|list< <numberOfRequests>");
				return;
			}
		} catch (NumberFormatException e) {
			System.err.println("Usage: Client [serverNumber] <put|get|del|cas|list< <numberOfRequests>");
			return;
		}

		String operation = args.length == 3 ? args[1] : args[0];

		if (!operation.equals("put") && !operation.equals("get") && !operation.equals("del") && !operation.equals("cas")
				&& !operation.equals("list")) {
			System.err.println("Usage: Client [serverNumber] <put|get|del|cas|list< <numberOfRequests>");
			return;
		}

		try (InputStream input = new FileInputStream("./config.conf")) {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
			while (bufferedReader.ready())
				servers.add(bufferedReader.readLine());
			if (args.length == 3) {
				serverNumber = Integer.parseInt(args[0]);
			} else {
				serverNumber = new Random().nextInt(servers.size());
			}
			serverPort = Integer.parseInt(servers.get(serverNumber));
		} catch (IOException e) {
			System.err.println("Error starting Client");
			return;
		}

		if (!connect())
			return;

		String result;
		for (int i = 0; i < numberOfRequests; i++) {
			boolean completed = false;
			while (!completed)
				try {
					switch (operation) {
					case "p":
					case "put":
						System.out.println(">>> put " + "k" + i + " v" + Integer.toString(i));
						outStream.writeObject(
								new Command(Command.OP_TYPE.PUT, "k" + i, "v" + Integer.toString(i), null));
						result = (String) inStream.readObject();
						if (result.equals("-1"))
							System.err.println("The server returned with an error, the operation was unsuccessful");
						break;
					case "g":
					case "get":
						System.out.println(">>> get " + "k" + i);
						outStream.writeObject(new Command(Command.OP_TYPE.GET, "k" + i, null, null));
						result = (String) inStream.readObject();
						if (result.equals("-1"))
							System.err.println("The server returned with an error, the operation was unsuccessful");
						else
							System.out.println(result);
						break;
					case "d":
					case "del":
						System.out.println(">>> del " + "k" + i);
						outStream.writeObject(new Command(Command.OP_TYPE.DEL, "k" + i, null, null));
						result = (String) inStream.readObject();
						if (result.equals("-1"))
							System.err.println("The server returned with an error, the operation was unsuccessful");
						break;

					case "c":
					case "cas":
						System.out.println(
								">>> cas " + "k" + i + " v" + Integer.toString(i) + " v" + Integer.toString(i + 1));
						outStream.writeObject(
								new Command(Command.OP_TYPE.CAS, "k" + i, "v" + Integer.toString(i + 1), "v" + i));
						result = (String) inStream.readObject();
						if (result.equals("-1"))
							System.err.println("The server returned with an error, the operation was unsuccessful");
						break;
					case "l":
					case "list":
					default:
						System.out.println(">>> list");
						outStream.writeObject(new Command(Command.OP_TYPE.LIST, null, null, null));
						result = (String) inStream.readObject();
						if (result.equals("-1"))
							System.err.println("The server returned with an error, the operation was unsuccessful");
						else
							System.out.println(result);
						break;
					}
					completed = true;
				} catch (Exception e) {
					if (!connect())
						return;
					continue;
				}
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
