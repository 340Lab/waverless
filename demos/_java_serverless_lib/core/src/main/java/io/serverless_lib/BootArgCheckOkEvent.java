package io.serverless_lib;

public class BootArgCheckOkEvent {

    String agentSock;
    String appName;
    String httpPort;

    public BootArgCheckOkEvent(String agentSock,String appName, String httpPort) {
        this.agentSock = agentSock;
        this.appName = appName;
        this.httpPort = httpPort;
    }
}
