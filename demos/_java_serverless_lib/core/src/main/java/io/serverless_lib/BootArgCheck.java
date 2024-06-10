package io.serverless_lib;

import org.springframework.stereotype.Component;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.core.env.Environment;

@Component
public class BootArgCheck implements CommandLineRunner
// DisposableBean
{
    public BootArgCheck() {
        super();
        System.out.println("UDSFuncAgentCommu construct");
    }

    Thread netty_thread = null;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private BeanConfig beanConfig;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    private Environment environment;

    String agentSock = "";

    @Override
    public void run(String... _args) throws Exception {
        System.out.println("BootArgCheck run");
        ApplicationArguments args = new DefaultApplicationArguments(_args);

        String[] agentsock = { "" };
        if (args.containsOption("agentSock")) {
            System.out.println("Option agentSock is present: " + args.getOptionValues("agentSock"));
            agentsock[0] = args.getOptionValues("agentSock").get(0);
        } else {
            // throw new IllegalArgumentException("No arguments provided, application cannot
            // start.");
            if (context != null) {
                System.err.println("No arguments provided, application cannot start.");
                ((ConfigurableApplicationContext) context).close();
                System.exit(0);
            }
        }

        String port = environment.getProperty("local.server.port");

        // beanConfig.bootArgReady();
        applicationEventPublisher.publishEvent(new BootArgCheckOkEvent(agentsock[0], port));
    }
}
