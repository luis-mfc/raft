import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class Log {

    public int CHUNK_SIZE;
    private int MAX_ENTRIES;
    private int MAX_ENTRIES_BEFORE_SNAPSHOT;
    private int term;
    private int index;
    private int commitIndex;
    private int serverId;
    private int lastIncludedIndex; // index of last snapshoted index
    private int lastIncludedTerm; // term of last snapshoted index
    private Integer clientId;
    private boolean snapshotReadingInProgress; // if true don't create a new one
    private List<Entry> entries;
    private Map<String, String> stateMachine;

    public Log(int id) {
        try (InputStream inputProperties = new FileInputStream("./config.properties")) {
            Properties properties = new Properties();
            properties.load(inputProperties);
            this.CHUNK_SIZE = Integer.parseInt(properties.getProperty("snapshot.chunkSize"));
            this.MAX_ENTRIES = Integer.parseInt(properties.getProperty("log.maxEntries"));
            this.MAX_ENTRIES_BEFORE_SNAPSHOT = Integer.parseInt(properties.getProperty("log.maxEntriesBeforeSnapShot"));
        } catch (IOException e) {
            // In case of error reading from Properties assume default values
            if (CHUNK_SIZE <= 0)
                CHUNK_SIZE = 1500;
            if (MAX_ENTRIES <= 0)
                MAX_ENTRIES = 1000;
            if (MAX_ENTRIES_BEFORE_SNAPSHOT <= 0)
                MAX_ENTRIES_BEFORE_SNAPSHOT = 1000;
        }
        this.term = 0;
        this.index = 0;
        this.commitIndex = 0;
        this.serverId = id;
        this.lastIncludedIndex = 0;
        this.lastIncludedTerm = 0;
        this.clientId = 0;
        this.entries = new ArrayList<>();
        this.stateMachine = new HashMap<>();
        this.snapshotReadingInProgress = false;
        readFromFiles();
    }

    public void updateCommitIndex(int newCommitIndex) {
        this.commitIndex = newCommitIndex;
        updateLogInfoFile();
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void deleteEntries(int index) {
        for (int i = this.index - 1 - lastIncludedIndex; i >= index - 1 - lastIncludedIndex; i--) {
            entries.remove(i);
        }
        this.index = entries.size();
    }

    public int incrementTerm() {
        term++;
        updateLogInfoFile();
        return term;
    }

    // @returns the index where the new Entry was appended
    public synchronized int appendEntry(Entry entry) {
        synchronized (entries) {
            entries.add(entry);
        }
        this.index++;
        log(entry);
        return this.index;
    }

    public void appendEntries(List<Entry> entries) {
        if (entries.isEmpty())
            return;
        synchronized (entries) {
            for (Entry entry : entries) {
                if (entry.getRequest().getClientId() > clientId) {
                    clientId = entry.getRequest().getClientId();
                    updateLogInfoFile();
                }
                this.entries.add(entry);
            }
        }
        log(entries, true);
        index = entries.get(entries.size() - 1).getIndex();
    }

    public List<Entry> getEntriesAfterIndex(int index) {
        if (index <= lastIncludedIndex) {
            return null;
        }
        List<Entry> result = new ArrayList<>();
        for (int i = index - 1 - lastIncludedIndex; i < (this.index - lastIncludedIndex)
                && (result.size() < MAX_ENTRIES); i++) {
            result.add(entries.get(i));
        }
        return result;
    }

    public int getTerm(int index) {
        if (index == lastIncludedIndex)
            return lastIncludedTerm;
        return entries.get(index - 1 - lastIncludedIndex).getTerm();
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public Integer getClientId() {
        return clientId;
    }

    public void incrementClientId() {
        this.clientId++;
        updateLogInfoFile();
    }

    public Map<String, String> getStateMachine() {
        return stateMachine;
    }

    public Entry getEntry(int index) {
        if (index == lastIncludedIndex)
            return null;
        return entries.get(index - 1 - lastIncludedIndex);
    }

    public boolean isEmpty() {
        return index == 0;
    }

    public boolean hasEntry(int index) {
        boolean result = true;
        try {
            if (index <= lastIncludedIndex)
                return true;
            entries.get(index - 1 - lastIncludedIndex);
        } catch (IndexOutOfBoundsException e) {
            result = false;
        }
        return result;
    }

    // @param index index to startExecution (should be the last executed)
    public void executeEntries(int index) {
        for (int i = index; i < commitIndex; i++) {
            Command command = entries.get(i - lastIncludedIndex).getRequest().getCommand();
            switch (command.getType()) {
            case PUT:
                stateMachine.put(command.getKey(), command.getValue());
                break;
            case DEL:
                stateMachine.remove(command.getKey());
                break;
            case CAS:
                if (stateMachine.containsKey(command.getKey())
                        && stateMachine.get(command.getKey()).equals(command.getPrevValue()))
                    stateMachine.put(command.getKey(), command.getValue());
                break;
            default:
                break;
            }
        }
        checkSnapshot();
    }

    public String executeAndGet(int index) {
        Command command = entries.get(index - lastIncludedIndex).getRequest().getCommand();
        String result;
        switch (command.getType()) {
        case PUT:
            stateMachine.put(command.getKey(), command.getValue());
            result = "0";
            break;
        case GET:
            result = stateMachine.containsKey(command.getKey()) ? stateMachine.get(command.getKey()) : "-1";
            break;
        case DEL:
            result = stateMachine.remove(command.getKey()) != null ? "0" : "-1";
            break;
        case CAS:
            if (stateMachine.containsKey(command.getKey())
                    && stateMachine.get(command.getKey()).equals(command.getPrevValue())) {
                stateMachine.put(command.getKey(), command.getValue());
                result = "0";
            } else
                result = "-1";
            break;
        default:
        case LIST:
            result = stateMachine.toString();
            break;
        }
        checkSnapshot();
        return result;
    }

    private void log(List<Entry> entries, boolean append) {
        File dir = new File("LOG" + serverId);
        dir.mkdirs();
        File file = new File("LOG" + serverId + "/" + "log");
        ObjectOutputStream os;
        try {
            if (!file.exists()) {
                file.createNewFile();
                os = new ObjectOutputStream(new FileOutputStream(file, append));
            } else {
                os = new ObjectOutputStream(new FileOutputStream(file, append)) {
                    protected void writeStreamHeader() throws IOException {
                        reset();
                    }
                };
            }
            for (Entry entry : entries) {
                os.writeObject(entry);
            }
            os.close();
        } catch (IOException e) {
        }
    }

    private void log(Entry entry) {
        File dir = new File("LOG" + serverId);
        dir.mkdirs();
        File file = new File("LOG" + serverId + "/" + "log");
        ObjectOutputStream os;
        try {
            if (!file.exists()) {
                file.createNewFile();
                os = new ObjectOutputStream(new FileOutputStream(file, true));
            } else {
                os = new ObjectOutputStream(new FileOutputStream(file, true)) {
                    protected void writeStreamHeader() throws IOException {
                        reset();
                    }
                };
            }
            os.writeObject(entry);
            os.close();
        } catch (IOException e) {
        }
    }

    private void checkSnapshot() {
        if (commitIndex - lastIncludedIndex > MAX_ENTRIES_BEFORE_SNAPSHOT && !snapshotReadingInProgress) {
            File file = new File("LOG" + serverId + "/" + "log");
            if (!file.delete())
                System.err.println("Error deleting the log file");
            else {
                createNewSnapShot();
            }
        }
    }

    private void createNewSnapShot() {
        this.lastIncludedTerm = entries.get(commitIndex - 1 - lastIncludedIndex).getTerm();
        this.lastIncludedIndex = commitIndex;
        File dir = new File("LOG" + serverId);
        dir.mkdirs();
        File file = new File("LOG" + serverId + "/" + "snapshotMetadata");
        try {
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
            writer.write("lastIncludedIndex=" + index + "\n");
            writer.write("lastIncludedTerm=" + entries.get(entries.size() - 1).getTerm() + "\n");
            writer.close();
        } catch (IOException e) {
        }
        file = new File("LOG" + serverId + "/" + "snapshot");
        try {
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
            Iterator<java.util.Map.Entry<String, String>> iterator = stateMachine.entrySet().iterator();
            while (iterator.hasNext()) {
                java.util.Map.Entry<String, String> entry = iterator.next();
                writer.append(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
        }
        file = new File("LOG" + serverId + "/" + "log");
        file.delete();
        synchronized (entries) {
            entries.clear();
        }
    }

    // @returns number of bytes read
    public byte[] getChunk(int chunk) throws IOException {
        File file = new File("LOG" + serverId + "/" + "snapshot");
        FileInputStream fileInputStream = new FileInputStream(file);
        long fileSize = file.length();
        // file ended
        if (fileSize - CHUNK_SIZE * chunk == 0) {
            fileInputStream.close();
            return new byte[0];
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) Math.min(CHUNK_SIZE, fileSize - CHUNK_SIZE * chunk));
        fileInputStream.getChannel().read(bytes, CHUNK_SIZE * chunk);

        byte[] result = bytes.array();
        fileInputStream.close();
        return result;

    }

    public void installSnapshot(InstallSnapshot installSnapshot) {
        // this.index = entries.isEmpty() ? lastIncludedIndex :
        // entries.get(entries.size() - 1).getIndex();
        System.out.println(installSnapshot);
        File dir = new File("LOG" + serverId);
        dir.mkdirs();
        File file = new File("LOG" + serverId + "/" + "snapshot");

        if (installSnapshot.getData().length != 0)
            try {
                boolean append = true;
                // 2. Create new snapshot file if first chunk (offset is 0)
                if (installSnapshot.getOffset() == 0) {
                    file.createNewFile();
                    append = false;
                }
                // 3. Write data into snapshot file at given offset
                FileOutputStream fileOutputStream = new FileOutputStream(file, append);
                fileOutputStream.write(installSnapshot.getData());
                fileOutputStream.close();
            } catch (IOException e) {
                // System.err.println("Error writing to the log");
            }
        if (installSnapshot.isDone()) {
            // 6. If existing log entry has same index and term as snapshot’s last included
            // entry, retain log entries following it and reply
            boolean found = false;
            for (int i = 0; i < entries.size() && !found; i++) {
                Entry entry = entries.get(i);
                if (entry.getIndex() == installSnapshot.getLastIncludedIndex()
                        && entry.getTerm() == installSnapshot.getLastIncludedTerm()) {
                    if ((i + 1) < entries.size()) {
                        entries = entries.subList(i + 1, entries.size() - 1);
                        found = true;
                    }
                }
            }
            // 7. Discard the entire log
            synchronized (entries) {
                if (!found)
                    entries.clear();
            }

            // 8. Reset state machine using snapshot contents (and loadsnapshot’s cluster
            // configuration)
            readSnapShot();

            File metadataFile = new File("LOG" + serverId + "/" + "snapshotMetadata");
            try {
                metadataFile.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(metadataFile, false));
                writer.write("lastIncludedIndex=" + lastIncludedIndex + "\n");
                writer.write("lastIncludedTerm=" + lastIncludedTerm + "\n");
                writer.close();
            } catch (IOException e) {
            }
            lastIncludedIndex = installSnapshot.getLastIncludedIndex();
            lastIncludedTerm = installSnapshot.getLastIncludedTerm();
            commitIndex = lastIncludedIndex;
            index = lastIncludedIndex;
            System.out.println(toString());
        }
    }

    private void readSnapShot() {
        File file = new File("LOG" + serverId + "/" + "snapshot");
        try (Scanner scanner = new Scanner(file)) {
            scanner.useDelimiter("\n");
            while (scanner.hasNext()) {
                String[] entry = scanner.next().split(" ");
                stateMachine.put(entry[0], entry[1]);
            }
            scanner.close();
        } catch (IOException e) {
            return;
        }
    }

    private void updateLogInfoFile() {
        File dir = new File("LOG" + serverId);
        dir.mkdirs();
        File file = new File("LOG" + serverId + "/" + "logInfo");
        try {
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
            writer.write("term=" + term);
            writer.newLine();
            writer.write("clientId=" + clientId);
            writer.close();
        } catch (IOException e) {
        }
    }

    private void readFromFiles() {
        // Save previous values to rollback if one of the files isn't correct
        int oldTerm = term;
        int oldIndex = index;
        int oldCommitIndex = commitIndex;
        int oldClientId = clientId;
        File file = new File("LOG" + serverId + "/" + "logInfo");
        if (file.exists())
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                this.term = Integer.parseInt(line.substring(line.indexOf("=") + 1));
                line = reader.readLine();
                this.clientId = Integer.parseInt(line.substring(line.indexOf("=") + 1));
                reader.close();
            } catch (Exception e) {
                return;
            }
        else {
            return;
        }

        file = new File("LOG" + serverId + "/" + "snapshotMetadata");
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                this.lastIncludedIndex = Integer.parseInt(line.substring(line.indexOf("=") + 1));
                index = lastIncludedIndex;
                line = reader.readLine();
                this.lastIncludedTerm = Integer.parseInt(line.substring(line.indexOf("=") + 1));
                reader.close();
            } catch (Exception e) {
                return;
            }

            file = new File("LOG" + serverId + "/" + "snapshot");
            if (file.exists())
                try (Scanner scanner = new Scanner(file)) {
                    scanner.useDelimiter("\n");
                    while (scanner.hasNext()) {
                        String[] entry = scanner.next().split(" ");
                        stateMachine.put(entry[0], entry[1]);
                    }
                    scanner.close();
                } catch (IOException e) {
                    index = oldIndex;
                    commitIndex = oldCommitIndex;
                    term = oldTerm;
                    clientId = oldClientId;
                    stateMachine.clear();
                    return;
                }
            else {
                index = oldIndex;
                commitIndex = oldCommitIndex;
                term = oldTerm;
                clientId = oldClientId;
                synchronized (entries) {
                    entries.clear();
                }
                return;
            }
        }

        file = new File("LOG" + serverId + "/" + "log");
        if (file.exists())
            try (

                    FileInputStream fileInputStream = new FileInputStream(file)) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                while (true) {
                    if (fileInputStream.available() != 0) {
                        Entry entry = (Entry) objectInputStream.readObject();
                        entries.add(entry);
                        this.index++;
                    } else {
                        break;
                    }
                }
            } catch (ClassNotFoundException | IOException e) {
                index = oldIndex;
                commitIndex = oldCommitIndex;
                term = oldTerm;
                clientId = oldClientId;
                entries.clear();
                return;
            }
        commitIndex = lastIncludedIndex;
    }

    @Override
    public String toString() {
        String result = new String();
        result += "---------------LOG---------------\n";
        result += "index: " + index + " commitIndex: " + commitIndex + " term: " + term + " lastIncludedIndex: "
                + lastIncludedIndex + " lastIncludedTerm: " + lastIncludedTerm + "\n";
        result += "Entries {";
        synchronized (entries) {
            for (Entry entry : entries) {
                result += entry.toString() + ",\n";
            }
        }
        result += "}";
        result += "\n---------------------------------\n";
        result += "State Machine: " + stateMachine;
        return result;
    }
}