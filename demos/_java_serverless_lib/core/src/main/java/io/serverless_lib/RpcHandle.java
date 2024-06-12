package io.serverless_lib;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

class RpcFuncMeta {
    private String methodName;
    private LinkedHashMap<String, Class<?>> parameters;
    private Class<?> returnType;

    public RpcFuncMeta(String methodName, Class<?> returnType, LinkedHashMap<String, Class<?>> parameters) {
        this.methodName = methodName;
        this.parameters = parameters;
        this.returnType = returnType;
    }

    // Getter 和 Setter 方法
    public String getMethodName() {
        return methodName;
    }

    public HashMap<String, Class<?>> getParameters() {
        return parameters;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public Class<?>[] getParameterTypes() {
        return parameters.values().toArray(new Class<?>[0]);
    }
}

public class RpcHandle<T> {
    private final Gson gson = new Gson();

    final protected HashMap<String, RpcFuncMeta> handleMeta = new HashMap<>();

    T service;

    public RpcHandle(T service) {
        this.service = service;
        this.createHandleMeta(service);
    }

    void createHandleMeta(T service) {
        Method[] methods = service.getClass().getDeclaredMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            Class<?> returnType = method.getReturnType();
            Parameter[] parameters = method.getParameters();
            LinkedHashMap<String, Class<?>> paramMap = new LinkedHashMap<>();
            for (Parameter parameter : parameters) {
                paramMap.put(parameter.getName(), parameter.getType());
            }
            RpcFuncMeta meta = new RpcFuncMeta(methodName, returnType, paramMap);
            handleMeta.put(methodName, meta);
        }
        // print the funcs
        for (Map.Entry<String, RpcFuncMeta> entry : handleMeta.entrySet()) {
            System.out.println("Func registered: " + entry.getKey());
        }
    }

    // protected abstract Object rpcDispatch(String func, Object requestObj);

    public String handleRpc(String func, String argStr) {
        // Get meta
        RpcFuncMeta rpcFunc = handleMeta.get(func);
        if (rpcFunc == null) {
            throw new IllegalArgumentException("Function not found: " + func);
        }

        // Deserialize request
        Object[] params;
        try {
            JsonObject jsonObject = gson.fromJson(argStr, JsonObject.class);

            // Extract parameters from JSON and convert to appropriate types
            Map<String, Class<?>> paramMeta = rpcFunc.getParameters();
            params = new Object[paramMeta.size()];
            int index = 0;
            for (Map.Entry<String, Class<?>> entry : paramMeta.entrySet()) {
                String paramName = entry.getKey();
                Class<?> paramType = entry.getValue();
                params[index++] = gson.fromJson(jsonObject.get(paramName), paramType);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize request", e);
        }

        // Call the method
        Object responseObj;
        try {
            Method method = service.getClass().getMethod(func, rpcFunc.getParameterTypes());
            responseObj = method.invoke(service, params);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke method", e);
        }

        // // Serialize response
        // ByteBuf responseBuf;
        // try {
        // String respJson = gson.toJson(responseObj);
        // byte[] respBytes = respJson.getBytes();
        // responseBuf = Unpooled.wrappedBuffer(respBytes);
        // } catch (Exception e) {
        // throw new RuntimeException("Failed to serialize response", e);
        // }

        return gson.toJson(responseObj);
    }
}
