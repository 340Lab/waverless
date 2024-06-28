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

    @Override
    public void run(String... _args) throws Exception {
        System.out.println("BootArgCheck run");
        ApplicationArguments args = new DefaultApplicationArguments(_args);

        String[] argsarr = { "agentSock","appName" };
        for(int i=0;i<argsarr.length;i++){
            if(args.containsOption(argsarr[i])){
                String arg=args.getOptionValues(argsarr[i]).get(0);
                System.out.println("Option "+argsarr[i]+" is present: " + arg);
                argsarr[i]=arg;
            }else{
                if (context != null) {
                    ((ConfigurableApplicationContext) context).close();
                }
                System.err.println("No arguments provided, application cannot start.");
                System.exit(0);
            }
        }

        String port = environment.getProperty("local.server.port");

        // beanConfig.bootArgReady();
        applicationEventPublisher.publishEvent(new BootArgCheckOkEvent(argsarr[0],argsarr[1], port));
    }
}
