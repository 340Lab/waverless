package io.serverless_lib;

public class RpcHandleOwner {
    public RpcHandle<?> rpcHandle = null;

    public <T> void register(T service) {
        System.out.println("RpcHandleOwner registered");
        rpcHandle = new RpcHandle<>(service);
    }
}
