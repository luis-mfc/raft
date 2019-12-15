import java.io.Serializable;

public class Request implements Serializable {

    private static final long serialVersionUID = 1L;
    private Command command;
    private int operationId;
    private int clientId;

    public Request(Command request, int operationId, int clientId) {
        this.command = request;
        this.operationId = operationId;
        this.clientId = clientId;
    }

    public Command getCommand() {
        return command;
    }

    public int getClientId() {
        return clientId;
    }

    public int getOperationId() {
        return operationId;
    }

    @Override
    public String toString() {
        return "Request [clientId=" + clientId + ", operationId=" + operationId + ", command=" + command + "]";
    }

    public String toLoggingString() {
        return "[clientId=" + clientId + ",operationId=" + operationId + ",command=" + command.toLoggingString() + "]";
    }
}