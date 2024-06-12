package io.serverless_lib;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanConfig {
    @Bean
    public BootArgCheck bootArgCheck() {
        return new BootArgCheck();
    }

    @Bean
    public UdsBackend udsBackend() {
        return new UdsBackend();
    }

    @Bean
    public RpcHandleOwner rpcHandleOwner() {
        return new RpcHandleOwner();
    }

    @Bean
    public CracManager cracManager() {
        return new CracManager();
    }
}
