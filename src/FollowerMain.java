import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

public class FollowerMain {

    static public void main(String args[]) {
        Follower follower;
        int port;
        try (InputStream input = new FileInputStream("./config.conf")) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
            List<Integer> ports = new ArrayList<>();
            while (bufferedReader.ready())
                ports.add(Integer.valueOf(bufferedReader.readLine()));
            if (ports.size() <= Integer.valueOf(args[0]) || Integer.valueOf(args[0]) < 0) {
                System.err.println("Invalid port number, check the configuration file");
                return;
            }
            port = ports.get(Integer.valueOf(args[0]));
            int[] serverPorts = ports.stream().mapToInt(i -> i).toArray();

            try (InputStream inputProperties = new FileInputStream("./config.properties")) {
                Properties properties = new Properties();
                properties.load(inputProperties);
                int minElectionTimeout = Integer.parseInt(properties.getProperty("follower.timeoutMinValue"));
                int maxElectionTimeout = Integer.parseInt(properties.getProperty("follower.timeoutMaxValue"));
                int heartbeatRate = Integer.parseInt(properties.getProperty("leader.heartbeatRate"));
                if (minElectionTimeout < 0 || maxElectionTimeout < 0 || heartbeatRate < 0
                        || maxElectionTimeout < minElectionTimeout) {
                    System.err.println("The properties file isn't properly configured");
                    System.exit(-1);
                }
                String level = properties.getProperty("verboseLevel");
                Level verboseLevel;
                if (level.equals("ELECTION"))
                    verboseLevel = CustomLevel.ELECTION;
                else if (level.equals("LOG_REPLICATION"))
                    verboseLevel = CustomLevel.LOG_REPLICATION;
                else if (level.equals("ALL"))
                    verboseLevel = CustomLevel.ALL;
                else if (level.equals("OFF"))
                    verboseLevel = CustomLevel.OFF;
                else
                    verboseLevel = CustomLevel.DEFAULT;
                follower = new Follower(port, minElectionTimeout, maxElectionTimeout, heartbeatRate, serverPorts,
                        verboseLevel);
                follower.start();
            }
        } catch (Exception e) {
            System.out.println("Error starting follower");
        }
    }
}