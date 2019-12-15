import java.util.logging.Level;

class CustomLevel extends Level {

    private static final long serialVersionUID = 1L;
    public static final Level ALL = Level.ALL;
    public static final Level ELECTION = new CustomLevel("ELECTION", 50);
    public static final Level LOG_REPLICATION = new CustomLevel("LOG_REPLICATION", 100);
    public static final Level DEFAULT = new CustomLevel("DEFAULT", 200);
    public static final Level OFF = new CustomLevel("OFF", 1200);

    public CustomLevel(String name, int level) {
        super(name, level);
    }
}