package io.serverless_lib;

public class BootArgCheckOkEvent {

    String agentSock;

    String httpPort;

    public BootArgCheckOkEvent(String agentSock, String httpPort) {
        this.agentSock = agentSock;
        this.httpPort = httpPort;
    }
}
