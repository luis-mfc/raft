import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Follower {

    private final static String EXIT_CODE = "EXIT";
    private final int port;
    private final int minElectionTimeout;
    private final int maxElectionTimeout;
    private final int heartbeatRate;
    private int currentElectionTimeout;
    private int[] serverPorts;

    private int leaderPort;
    private Integer votedFor;
    private boolean wonElection;
    private boolean giveUpElection;
    private CountDownLatch electionLatch;
    private Timer timer;

    private STATE state;
    private Log log;
    private ServerSocket serverSocket;

    private Logger logger;
    private Level verboseLevel;

    // Identify the source of the socket connection
    private enum COMMUNICATIONTYPE {
        ELECTION, LEADER, CLIENT
    }

    private enum STATE {
        LEADER, FOLLOWER, CANDIDATE
    }

    public Follower(int port, int minElectionTimeout, int maxElectionTimeout, int heartbeatRate, int[] serverPorts,
            Level verboseLevel) throws RemoteException {
        this.port = port;
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.heartbeatRate = heartbeatRate;
        this.serverPorts = serverPorts;
        this.currentElectionTimeout = 0;
        this.leaderPort = -1;
        this.votedFor = -1;
        this.wonElection = false;
        this.giveUpElection = false;
        this.electionLatch = new CountDownLatch(1);
        this.timer = null;
        this.state = STATE.FOLLOWER;
        this.log = new Log(port);
        this.serverSocket = null;
        this.verboseLevel = verboseLevel;
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
        logger.log(CustomLevel.LOG_REPLICATION, log.toString() + "\n\n");
    }

    public Follower(int port, int minElectionTimeout, int maxElectionTimeout, int heartbeatRate, int[] serverPorts,
            Level verboseLevel, Log log, ServerSocket serverSocket) throws RemoteException {
        this(port, minElectionTimeout, maxElectionTimeout, heartbeatRate, serverPorts, verboseLevel);
        this.serverSocket = serverSocket;
        this.log = log;
    }

    public void start() {
        try {
            if (serverSocket == null)
                serverSocket = new ServerSocket(port);
            ServerThread newServerThread = new ServerThread();
            newServerThread.start();
            logger.log(CustomLevel.DEFAULT, "Follower " + port + " running...\n\n");
        } catch (IOException e) {
            try {
                if (serverSocket != null)
                    serverSocket.close();
            } catch (IOException e1) {
            }
            logger.log(CustomLevel.DEFAULT, "Error setting up the server socket\n");
            System.exit(1);
        }
    }

    // Server Thread of The Consensus module, waits for new connections
    class ServerThread extends Thread {

        public void run() {
            // Start the first election timeout
            resetTimer();
            while (true) {
                final Socket socket;
                ObjectOutputStream outStream;
                ObjectInputStream inStream;
                COMMUNICATIONTYPE type;
                final Object object;
                try {
                    socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    outStream = new ObjectOutputStream(socket.getOutputStream());
                    inStream = new ObjectInputStream(socket.getInputStream());
                    object = inStream.readObject();

                    // Check the type of the incoming communication
                    if (object instanceof RequestVote)
                        type = COMMUNICATIONTYPE.ELECTION;
                    else if (object instanceof AppendEntries || object instanceof InstallSnapshot) {
                        type = COMMUNICATIONTYPE.LEADER;
                        resetTimer();
                    } else if (object instanceof Integer)
                        type = COMMUNICATIONTYPE.CLIENT;
                    else if (object instanceof String) {
                        // Exit this ServerSocket accept thread, the election was won
                        electionLatch.countDown();
                        return;
                    } else {
                        continue;
                    }
                } catch (ClassNotFoundException | IOException e) {
                    return;
                }
                Runnable task;
                if (type == COMMUNICATIONTYPE.ELECTION) {
                    task = new Runnable() {
                        public void run() {
                            try {
                                RequestVote requestVote = (RequestVote) object;
                                if (requestVote.getTerm() > log.getTerm()) {
                                    log.setTerm(requestVote.getTerm());
                                    synchronized (votedFor) {
                                        votedFor = -1;
                                    }
                                }
                                ElectionResult electionResult = null;
                                synchronized (votedFor) {
                                    if (wonElection)
                                        electionResult = new ElectionResult(log.getTerm(), false);

                                    if ((requestVote.getTerm() >= log.getTerm())
                                            && (votedFor == -1 || votedFor == requestVote.getCandidateId())
                                            && requestVote.getLastLogIndex() >= log.getIndex()
                                            && ((log.getIndex() == 0 && requestVote.getLastLogTerm() >= log.getTerm())
                                                    || (log.getIndex() != 0 && requestVote.getLastLogTerm() >= log
                                                            .getTerm(log.getIndex())))) {
                                        resetTimer();
                                        votedFor = requestVote.getCandidateId();
                                        electionResult = new ElectionResult(requestVote.getTerm(), true);
                                        log.setTerm(requestVote.getTerm());
                                        logger.log(CustomLevel.ELECTION, "Voted for " + requestVote + "\n");
                                    } else {
                                        electionResult = new ElectionResult(log.getTerm(), false);
                                        logger.log(CustomLevel.ELECTION, "Voted against " + requestVote + "\n");
                                    }
                                }
                                outStream.writeObject(electionResult);
                                socket.close();
                            } catch (IOException e) {
                                logger.log(CustomLevel.DEFAULT, "Error reading requestVote\n");
                                return;
                            }
                        }
                    };
                } else if (type == COMMUNICATIONTYPE.LEADER) {
                    task = new Runnable() {
                        public void run() {
                            AppendEntries appendEntries = null;
                            Object obj = object;
                            while (true) {
                                try {
                                    if (wonElection) {
                                        outStream.writeObject(new Response(log.getTerm(), false));
                                        socket.close();
                                        return;
                                    }
                                    resetTimer();

                                    if (obj == null)
                                        obj = inStream.readObject();
                                    if (obj instanceof InstallSnapshot) {
                                        InstallSnapshot installSnapshot = (InstallSnapshot) obj;
                                        if (installSnapshot.getTerm() > log.getTerm())
                                            log.setTerm(installSnapshot.getTerm());

                                        if (installSnapshot.getTerm() < log.getTerm()) {
                                            // 1. Reply immediately if term < currentTerm
                                            outStream.writeObject(Integer.valueOf(log.getTerm()));
                                            continue;
                                        }
                                        log.installSnapshot(installSnapshot);
                                        outStream.writeObject(log.getTerm());
                                        obj = null;
                                        continue;
                                    }

                                    appendEntries = (AppendEntries) obj;
                                    if (appendEntries.getTerm() >= log.getTerm()) {
                                        leaderPort = appendEntries.getLeaderId();
                                        state = STATE.FOLLOWER;
                                        giveUpElection = true;
                                        log.setTerm(appendEntries.getTerm());
                                        synchronized (votedFor) {
                                            votedFor = -1;
                                        }
                                    }
                                    // if the server started an election it should giveUp on if
                                    // the election isn't won yet
                                    giveUpElection = true;
                                    leaderPort = appendEntries.getLeaderId();

                                    Response response = appendEntries(appendEntries.getTerm(),
                                            appendEntries.getLeaderId(), appendEntries.getPrevLogIndex(),
                                            appendEntries.getPrevLogTerm(), appendEntries.getEntries(),
                                            appendEntries.getLeaderCommit());
                                    outStream.writeObject(response);
                                    appendEntries = null;
                                    obj = null;
                                } catch (ClassNotFoundException | IOException e) {
                                    try {
                                        socket.close();
                                    } catch (IOException e1) {
                                    }
                                    return;
                                }
                            }
                        }
                    };
                } else {
                    task = new Runnable() {
                        public void run() {
                            // Respond with the present leader
                            try {
                                if (leaderPort != -1)
                                    outStream.writeObject("127.0.0.1:" + leaderPort);
                                else
                                    outStream.writeObject("-1");
                                socket.close();
                            } catch (IOException e) {
                                logger.log(CustomLevel.DEFAULT, "Error dealing with a client\n");
                            }
                        }
                    };
                }
                Thread thread = new Thread(task);
                thread.start();
            }
        }
    }

    // Implements the logic of the response to an AppendEntries RPC
    public Response appendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<Entry> entries,
            int leaderCommit) throws RemoteException {

        if (term < log.getTerm() || (prevLogIndex != 0 && !log.hasEntry(prevLogIndex))
                || (prevLogIndex != 0 && log.getTerm(prevLogIndex) != prevLogTerm)) {
            return new Response(log.getTerm(), false);
        }
        if (log.hasEntry(prevLogIndex + 1)) {
            log.deleteEntries(prevLogIndex + 1);
        }
        log.appendEntries(entries);

        if (leaderCommit > log.getCommitIndex()) {
            int oldCommitIndex = log.getCommitIndex();
            if (!entries.isEmpty()) {
                log.updateCommitIndex(Math.min(leaderCommit, entries.get(entries.size() - 1).getIndex()));
                log.executeEntries(oldCommitIndex);
            } else {
                log.updateCommitIndex(Math.min(leaderCommit, log.getIndex()));
                log.executeEntries(oldCommitIndex);
            }
            logger.log(CustomLevel.LOG_REPLICATION, log.toString() + "\n\n");
        }
        return new Response(log.getTerm(), true);
    }

    public void startElection() {
        leaderPort = -1;
        if (state == STATE.LEADER)
            return;
        giveUpElection = false;
        this.state = STATE.CANDIDATE;
        int electionTerm = log.incrementTerm();
        int lastLogIndex = log.getIndex();
        int lastLogTerm = lastLogIndex == 0 ? electionTerm : log.getTerm(lastLogIndex);
        synchronized (votedFor) {
            votedFor = port;
        }
        RequestVote requestVote = new RequestVote(electionTerm, port, lastLogIndex, lastLogTerm);
        logger.log(CustomLevel.ELECTION, "Timeout expired (" + currentElectionTimeout
                + "(ms)), started election of term " + requestVote.getTerm() + "\n\n");
        AtomicInteger atomicInteger = new AtomicInteger(1);
        int majority = 1 + (int) Math.ceil(this.serverPorts.length / 2);
        resetTimer();

        // If the server is only one in the cluster it automaticaly wins the election
        if (majority == 1) {
            if (giveUpElection) {
                return;
            }
            timer.cancel();
            state = STATE.LEADER;
            wonElection = true;
            leaderPort = Follower.this.port;
            try {
                Socket socket = new Socket("localhost", Follower.this.port);
                socket.setTcpNoDelay(true);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(EXIT_CODE);
                try {
                    electionLatch.await();
                } catch (InterruptedException e) {
                }
                socket.close();
            } catch (IOException e) {
            }
            for (Handler handler : logger.getHandlers()) {
                logger.removeHandler(handler);
            }
            Leader leader = new Leader(Follower.this.port, serverPorts, heartbeatRate, log, serverSocket, verboseLevel);
            leader.start();

            // else start the proper election
        } else {
            for (int port : serverPorts) {
                if (port == this.port)
                    continue;
                final int serverPort = port;
                // If the majority way already achived there is not point to send more
                // RequestVotes
                if (atomicInteger.get() > majority) {
                    continue;
                }
                Thread requestVoteThread = new Thread(new Runnable() {
                    public void run() {
                        try {
                            if (log.getTerm() > electionTerm) {
                                return;
                            }
                            Socket server = new Socket("localhost", serverPort);
                            server.setTcpNoDelay(true);
                            ObjectOutputStream outStream = new ObjectOutputStream(server.getOutputStream());
                            ObjectInputStream inStream = new ObjectInputStream(server.getInputStream());
                            outStream.writeObject(requestVote);
                            ElectionResult electionResult = (ElectionResult) inStream.readObject();
                            if (electionResult.getVoteGranted()) {
                                if (atomicInteger.incrementAndGet() == majority) {
                                    if (giveUpElection) {
                                        server.close();
                                        return;
                                    }
                                    timer.cancel();
                                    state = STATE.LEADER;
                                    wonElection = true;
                                    leaderPort = Follower.this.port;
                                    Socket socket = new Socket("localhost", Follower.this.port);
                                    socket.setTcpNoDelay(true);
                                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                                            socket.getOutputStream());
                                    objectOutputStream.writeObject(EXIT_CODE);
                                    try {
                                        electionLatch.await();
                                    } catch (InterruptedException e) {
                                    }
                                    socket.close();
                                    for (Handler handler : logger.getHandlers()) {
                                        logger.removeHandler(handler);
                                    }
                                    Leader leader = new Leader(Follower.this.port, serverPorts, heartbeatRate, log,
                                            serverSocket, verboseLevel);
                                    leader.start();
                                }
                            } else {
                                if (electionResult.getTerm() > log.getTerm()) {
                                    log.setTerm(electionResult.getTerm());
                                }
                            }
                            server.close();
                        } catch (ClassNotFoundException | IOException e) {
                            return;
                        }
                    }
                });
                requestVoteThread.start();
            }
        }
    }

    private synchronized void resetTimer() {
        if (minElectionTimeout != maxElectionTimeout)
            currentElectionTimeout = new Random().nextInt(maxElectionTimeout - minElectionTimeout) + minElectionTimeout;
        else
            currentElectionTimeout = minElectionTimeout;
        if (timer != null)
            timer.cancel();
        timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                startElection();
            }
        }, currentElectionTimeout);
    }
}