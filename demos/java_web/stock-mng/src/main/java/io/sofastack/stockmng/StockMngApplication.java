package io.sofastack.stockmng;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"io.sofastack.stockmng","io.serverless_lib"})
public class StockMngApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockMngApplication.class, args);
    }

}
