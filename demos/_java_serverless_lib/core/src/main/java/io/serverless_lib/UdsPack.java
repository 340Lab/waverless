package io.serverless_lib;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.annotation.PostConstruct;
import process_rpc_proto.ProcessRpcProto.AppStarted;
import process_rpc_proto.ProcessRpcProto.FuncCallReq;
import process_rpc_proto.ProcessRpcProto.UpdateCheckpoint;
import process_rpc_proto.ProcessRpcProto.FuncCallResp;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.channel.unix.UnixChannel;
import io.netty.buffer.Unpooled;
import org.springframework.stereotype.Component;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;


public class UdsPack{
    Object pack;
    public int id;
    public int taskId;
    public UdsPack(Object inner,int taskId){
        pack=inner;
        this.taskId=taskId;

        if(inner instanceof FuncCallResp){
            id=3;
        }
        else if(inner instanceof UpdateCheckpoint){
            id=4; 
        }else{
            throw new IllegalArgumentException("Unknown pack type");
        }
    }
    
    byte[] staticEncode(){
        switch(id){
            case 3:
                return ((FuncCallResp)pack).toByteArray();
            case 4:
                return ((UpdateCheckpoint)pack).toByteArray();
            default:
                throw new IllegalArgumentException("Unknown pack type");
        }
    }

    public ByteBuf encode(){
        byte[] data = staticEncode();
        ByteBuf buffer = Unpooled.buffer(9 + data.length);
        buffer.writeInt(data.length); // length
        byte[] msgId = {(byte)id}; 
        buffer.writeBytes(msgId);// pack type
        buffer.writeInt(taskId);// dummy task id
        buffer.writeBytes(data);// data

        return buffer;
    }
}