import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Leader {

	private final int port;
	private final int numberOfFollowers;
	private final int heartbeatRate;
	private Integer operationId;

	private FollowerInfo[] followers;

	// Saves the client OutputStreams to send confirmation
	// of execution of requests
	private Map<Integer, ObjectOutputStream> clients;
	private Object[] requestLocks;
	private Object commitIndexLock;
	private CountDownLatch lostTermLatch;
	private Log log;
	private ServerSocket serverSocket;

	// true if a leader with a superior term has contacted this server,
	// if so switch to follower
	private boolean lostTerm;

	private Logger logger;
	private Level verboseLevel;

	class FollowerInfo {
		private Socket socket;
		private ObjectOutputStream outStream;
		private ObjectInputStream inStream;
		private int index;
		private int port;

		public FollowerInfo(int port) {
			socket = null;
			outStream = null;
			inStream = null;
			index = log.getCommitIndex();
			this.port = port;
		}
	};

	public Leader(int port, int[] ports, int heartbeatRate, Log log, ServerSocket serverSocket, Level verboseLevel) {
		this.port = port;
		this.heartbeatRate = heartbeatRate;
		this.commitIndexLock = new Object();
		this.numberOfFollowers = ports.length - 1;
		this.log = log;
		this.followers = new FollowerInfo[numberOfFollowers];
		this.clients = new HashMap<>();
		this.requestLocks = new Object[numberOfFollowers];
		this.operationId = (log.getIndex() > log.getLastIncludedIndex()) ? log.getIndex() : log.getLastIncludedIndex();
		int j = 0;
		for (int i = 0; i < ports.length; i++) {
			if (ports[i] == port) {
				j = -1;
				continue;
			}
			this.followers[i + j] = new FollowerInfo(ports[i]);
			this.requestLocks[i + j] = new Object();
		}
		this.serverSocket = serverSocket;
		this.lostTermLatch = new CountDownLatch(numberOfFollowers);
		this.lostTerm = false;
		this.logger = Logger.getLogger(this.getClass().getName() + port);
		this.logger.setUseParentHandlers(false);
		this.logger.setLevel(verboseLevel);
		ConsoleHandler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(verboseLevel);
		consoleHandler.setFormatter(new SimpleFormatter() {
			@Override
			public synchronized String format(LogRecord lr) {
				return lr.getMessage();
			}
		});
		this.logger.addHandler(consoleHandler);
		this.verboseLevel = verboseLevel;
	}

	public void start() {
		try {
			// Create threads to communicate to the followers
			for (int i = 0; i < numberOfFollowers; i++) {
				new FollowerCommunicationThread(i).start();
			}
			// Create the Thread to receive client requests
			if (serverSocket == null)
				serverSocket = new ServerSocket(port);
			new ServerThread().start();
			logger.log(CustomLevel.DEFAULT, "Leader (term " + log.getTerm() + ") running...\n\n");
		} catch (IOException e) {
			logger.log(CustomLevel.DEFAULT, "Error creating the server socket\n");
			System.exit(1);
		}
	}

	// Server Thread of The Consensus module, waits for new connections
	class ServerThread extends Thread {

		public void run() {
			while (!lostTerm) {
				final Socket socket;
				int clientId = -1;
				final ObjectOutputStream outStream;
				final ObjectInputStream inStream;
				final Object object;
				try {
					socket = serverSocket.accept();
					socket.setTcpNoDelay(true);
					outStream = new ObjectOutputStream(socket.getOutputStream());
					inStream = new ObjectInputStream(socket.getInputStream());

					object = inStream.readObject();

					// Check communication type

					if (object instanceof AppendEntries) { // Other Leader
						AppendEntries appendEntries = ((AppendEntries) object);
						if (appendEntries.getTerm() > log.getTerm()) {
							outStream.writeObject(new Response(log.getTerm(), false));
							socket.close();
							Leader.this.lostTerm = true;
							log.setTerm(appendEntries.getTerm());
							for (int i = 0; i < requestLocks.length; i++) {
								synchronized (requestLocks[i]) {
									requestLocks[i].notify();
								}
							}
							try {
								lostTermLatch.await();
							} catch (InterruptedException e) {
								logger.log(CustomLevel.DEFAULT, "Error waiting for the followerThreads to end\n");
							}
							// Become a Follower
							try (InputStream inputProperties = new FileInputStream("./config.properties")) {
								Properties properties = new Properties();
								properties.load(inputProperties);
								int minElectionTimeout = Integer
										.parseInt(properties.getProperty("follower.timeoutMinValue"));
								int maxElectionTimeout = Integer
										.parseInt(properties.getProperty("follower.timeoutMaxValue"));
								int heartbeatRate = Integer.parseInt(properties.getProperty("leader.heartbeatRate"));
								int[] ports = new int[numberOfFollowers + 1];
								int j;
								for (j = 0; j < numberOfFollowers; j++) {
									ports[j] = followers[j].port;
								}
								ports[j] = port;

								for (Handler handler : logger.getHandlers()) {
									logger.removeHandler(handler);
								}

								Follower follower = new Follower(port, minElectionTimeout, maxElectionTimeout,
										heartbeatRate, ports, verboseLevel, log, serverSocket);
								follower.start();
								return;
							}
						} else {
							outStream.writeObject(new Response(log.getTerm(), false));
						}
						continue;
					} else if (object instanceof RequestVote) { // Other server requesting a vote
						outStream.writeObject(new ElectionResult(log.getTerm(), false));
						continue;
					}
					// A new Client
					clientId = (int) object;

					// -1 means the client does not have an id
					if (clientId == -1) {
						// Send Id do client
						log.incrementClientId();
						outStream.writeObject(log.getClientId().toString());
						clientId = log.getClientId();
					} else {
						outStream.writeObject(Integer.valueOf(clientId).toString());
					}
				} catch (IOException | ClassNotFoundException e) {
					if (clientId != -1)
						logger.log(CustomLevel.DEFAULT, "Error connecting to the client " + clientId + "\n");
					else
						logger.log(CustomLevel.DEFAULT, "Error connecting to a client\n");
					continue;
				}
				final int c = clientId;
				Thread ClientThread = new Thread() {
					public void run() {
						clients.put(c, outStream);
						int currentTerm = log.getTerm();
						while (!lostTerm) {
							try {
								// Receive a client request
								Request request = new Request((Command) inStream.readObject(), operationId, c);

								// if numberOfFollowers == 0 then the majority is automaticaly guaranteed
								if (numberOfFollowers == 0) {
									synchronized (commitIndexLock) {
										int index = log
												.appendEntry(new Entry(currentTerm, log.getIndex() + 1, request));
										log.updateCommitIndex(index);
										if (clients.containsKey(request.getClientId()))
											clients.get(request.getClientId())
													.writeObject(log.executeAndGet(index - 1));
										else
											log.executeAndGet(index - 1);
									}
								} else {
									log.appendEntry(new Entry(currentTerm, log.getIndex() + 1, request));
									// Notify each FollowerCommunicationThread to send new entries
									for (int i = 0; i < requestLocks.length; i++) {
										synchronized (requestLocks[i]) {
											requestLocks[i].notify();
										}
									}
								}
								synchronized (operationId) {
									operationId++;
								}
							} catch (ClassNotFoundException | IOException e) {
								// logger.log(CustomLevel.DEFAULT, "The Client disconnected\n");
								return;
							}
						}
					}
				};
				ClientThread.start();
			}
		}
	}

	class FollowerCommunicationThread extends Thread {

		private FollowerInfo follower;
		private int followerNumber;

		public FollowerCommunicationThread(int followerNumber) {
			follower = followers[followerNumber];
			this.followerNumber = followerNumber;
		}

		public void run() {
			Socket followerSocket = follower.socket;
			ObjectOutputStream outStream = null;
			ObjectInputStream inStream = null;
			Response response;
			int currentChunk = -1, currentOffset = 0;
			while (true) {
				try {
					if (lostTerm) {
						lostTermLatch.countDown();
						followerSocket.close();
						return;
					}
					if (followerSocket == null || followerSocket.isClosed()) {
						followerSocket = new Socket("localhost", follower.port);
						followerSocket.setTcpNoDelay(true);
						follower.outStream = new ObjectOutputStream(followerSocket.getOutputStream());
						follower.inStream = new ObjectInputStream(followerSocket.getInputStream());
						outStream = follower.outStream;
						inStream = follower.inStream;
					}
					List<Entry> entries = log.getEntriesAfterIndex(follower.index + 1);

					if (entries == null) {
						currentChunk++;
						// The Follower is behind, does not have some entries that are snapshoted
						byte[] data;
						boolean done;
						try {
							data = log.getChunk(currentChunk);
							done = data.length != log.CHUNK_SIZE || data.length == 0;
						} catch (IOException e) {
							data = new byte[0];
							done = false;
						}
						InstallSnapshot installSnapshot = new InstallSnapshot(log.getTerm(), port,
								log.getLastIncludedIndex(), log.getLastIncludedTerm(), data, currentOffset, done);
						currentOffset += data.length;
						if (done) {
							currentChunk = -1;
							currentOffset = 0;
							follower.index = log.getLastIncludedIndex();
						}
						outStream.writeObject(installSnapshot);
						int receivedTerm = (int) inStream.readObject();
						if (receivedTerm > log.getTerm())
							log.setTerm(receivedTerm);

					} else {
						int entriesSent = sendAppendEntries(follower, followerSocket, outStream, inStream, entries);
						response = (Response) inStream.readObject();

						if (response.getTerm() > log.getTerm())
							log.setTerm(response.getTerm());

						if (response.getSuccess()) {
							follower.index += entriesSent;
							int majority = 1 + (int) Math.ceil(((double) numberOfFollowers) / 2);
							int[] values = new int[majority - 1];

							// Update the commitIndex
							synchronized (commitIndexLock) {
								for (FollowerInfo f : followers) {
									for (int i = 0; i < values.length; i++) {
										if (values[i] < f.index) {
											values[i] = f.index;
											continue;
										}
									}
								}
								int oldCommitIndex = log.getCommitIndex();
								int newCommitIndex = Arrays.stream(values).min().getAsInt();
								if (newCommitIndex > oldCommitIndex) {
									log.updateCommitIndex(newCommitIndex);
									for (int i = oldCommitIndex; i < newCommitIndex; i++) {
										Entry entry = log.getEntry(i + 1);
										if (clients.containsKey(entry.getRequest().getClientId()))
											clients.get(entry.getRequest().getClientId())
													.writeObject(log.executeAndGet(i));
										else
											log.executeAndGet(i);
									}
									logger.log(CustomLevel.LOG_REPLICATION, log.toString() + "\n\n");
								}
							}
						} else {
							follower.index--;
						}
					}
					// In case more than heartBeatRate as passed from the last AppendEntries, dont
					// wait
					if (!lostTerm && (log.getIndex()) == follower.index) {
						try {
							synchronized (requestLocks[followerNumber]) {
								requestLocks[followerNumber].wait(heartbeatRate);
							}
						} catch (InterruptedException e) {
							logger.log(CustomLevel.DEFAULT,
									"Error when waiting for more entries to send to the follower " + followerNumber
											+ "\n");
						}
					}
				} catch (IOException | ClassNotFoundException e) {
					try {
						if (followerSocket != null && !followerSocket.isClosed())
							followerSocket.close();
					} catch (IOException e1) {
						logger.log(CustomLevel.DEFAULT, "Error closing socket of the follower " + follower.port + "\n");
					}
				}
			}
		}

	}

	// Implements the logic for building and sending a AppendEntries RPC
	// @return number of entries sent
	public int sendAppendEntries(FollowerInfo follower, Socket followerSocket, ObjectOutputStream outStream,
			ObjectInputStream inStream, List<Entry> entries) {
		try {
			AppendEntries appendEntry;
			int currentTerm = log.getTerm();
			if (entries.isEmpty()) {
				int term = follower.index == 0 ? log.getTerm() : log.getTerm(follower.index);
				appendEntry = new AppendEntries(currentTerm, port, follower.index, term, entries, log.getCommitIndex());
			} else {
				Entry first = entries.get(0);
				// If index == 1 (first entry) there is no prev
				if (first.getIndex() == 1)
					appendEntry = new AppendEntries(currentTerm, port, 0, currentTerm, entries, log.getCommitIndex());
				else {
					Entry prev = log.getEntry(first.getIndex() - 1);
					// if prev == null then prev is the last entry snapshoted
					int prevIndex = (prev == null) ? log.getLastIncludedIndex() : prev.getIndex();
					int prevtTerm = (prev == null) ? log.getLastIncludedTerm() : prev.getTerm();
					appendEntry = new AppendEntries(currentTerm, port, prevIndex, prevtTerm, entries,
							log.getCommitIndex());
				}
			}
			outStream.writeObject(appendEntry);
			return entries.size();
		} catch (IOException e) {
			try {
				if (followerSocket != null && !followerSocket.isClosed())
					followerSocket.close();
			} catch (IOException e1) {
				logger.log(CustomLevel.DEFAULT, "Error closing socket of the follower " + follower.port + "\n");
			}
			return 0;
		}
	}
}