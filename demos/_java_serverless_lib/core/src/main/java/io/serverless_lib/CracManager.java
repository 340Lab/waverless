package io.serverless_lib;

import org.springframework.stereotype.Component;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.core.env.Environment;
import process_rpc_proto.ProcessRpcProto.UpdateCheckpoint;

public class CracManager implements org.crac.Resource
// DisposableBean
{
    boolean checkpointed = false;

    @Autowired 
    UdsBackend uds;    

    @EventListener
    public void bootArgCheckOk(BootArgCheckOkEvent e) {
        //                                                 /-----------------------\
        //         /-------------------------------------- | # Agent               |
        //         |                                       \-----------------------/
        // /----------------------\
        // | # CracManager        |
        // | // first time init   |
        // | checkpointed = false |
        // \----------------------/
        //         |
        //         | bootArgCheckOk 
        //         | if (!checkpointed) ----- uds call -----> | 
        //                                                    |                
        //         | <----------------------------------------/ take snapshot         
        //         |                                       
        //         | beforeCheckpoint
        //         \ checkpointed = true--------------------> |
        //                                                    |
        // /----------------------\ <-------------------------/ restart by crac
        // | # CracManager        | 
        // | // criu init         |
        // | checkpointed = true  |
        // \----------------------/

        // register crac
        org.crac.Core.getGlobalContext().register(this);

        if (!checkpointed) {            
            System.out.println("CracManager requires for snapshot.");
            uds.send(new UdsPack(UpdateCheckpoint.newBuilder().build(),0));
            // TODO: change to rpc
            checkpointed = true;
        }
    }

    @Override
    public void beforeCheckpoint(org.crac.Context<? extends org.crac.Resource> context) throws Exception {
        // Prepare for checkpoint
        System.out.println("CracManager before checkpoint.");
        // close the uds
        uds.close();
    }

     @Override
    public void afterRestore(org.crac.Context<? extends org.crac.Resource> context) throws Exception {
        // Handle after restore
        System.out.println("CracManager after restore.");
        uds.start();
    }

}
