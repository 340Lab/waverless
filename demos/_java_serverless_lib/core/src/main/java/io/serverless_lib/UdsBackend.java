package io.serverless_lib;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.PostConstruct;
import process_rpc_proto.ProcessRpcProto.FuncStarted;
import process_rpc_proto.ProcessRpcProto.FuncCallReq;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
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

public class UdsBackend
// DisposableBean
{

    Thread netty_thread = null;

    @Autowired
    RpcHandleOwner rpcHandleOwner;

    @EventListener
    public void bootArgCheckOk(BootArgCheckOkEvent e) {
        netty_thread = new Thread(() -> {
            UnixChannelHandle.start(Paths.get(e.agentSock), e.httpPort, rpcHandleOwner);
        });
        netty_thread.start();
    }
}

class ByteBufInputStream extends InputStream {
    private final ByteBuf buffer;

    public ByteBufInputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        if (!buffer.isReadable()) {
            return -1;
        }
        return buffer.readByte() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
            return -1;
        }
        len = Math.min(len, readableBytes);
        buffer.readBytes(bytes, off, len);
        return len;
    }

    @Override
    public int available() throws IOException {
        return buffer.readableBytes();
    }
}

class UnixChannelHandle {
    static void waitingForSockFile(Path sock_path) {
        System.out.println("Current directory: " + Paths.get(".").toAbsolutePath().toString());
        while (true) {
            if (Files.exists(sock_path)) {
                System.out.println("Socket file exists: " + sock_path);
                break;
            } else {
                System.out.println("Socket file not exists: " + sock_path);
            }
            try {
                // 等待 1 秒钟后再次检查
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // 处理中断异常
                System.err.println("Thread was interrupted while waiting for the socket file.");
                Thread.currentThread().interrupt(); // 重新设置中断状态
                return;
            }
        }
    }

    static void start(Path sock_path, String httpPort, RpcHandleOwner rpcHandleOwner) {
        io.netty.bootstrap.Bootstrap bootstrap = new io.netty.bootstrap.Bootstrap();
        final EpollEventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup();
        try {
            bootstrap.group(epollEventLoopGroup)
                    .channel(EpollDomainSocketChannel.class)
                    .handler(new ChannelInitializer<UnixChannel>() {
                        @Override
                        public void initChannel(UnixChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
                                                throws Exception {
                                            FuncCallReq funcCallReq = FuncCallReq
                                                    .parseFrom(new ByteBufInputStream(msg));

                                            // Handle the deserialized message
                                            String func = funcCallReq.getFunc();
                                            String argStr = funcCallReq.getArgStr();

                                            // 需要一个线程池来处理消息
                                            rpcHandleOwner.rpcHandle.handleRpc(func, msg);
                                        }

                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            System.out.println("Channel is active");

                                            // Create AuthHeader message
                                            FuncStarted commu = FuncStarted.newBuilder().setFnid("stock-mng")
                                                    .setHttpPort(httpPort).build();

                                            // Serialize the message
                                            byte[] data = commu.toByteArray();

                                            System.err.println("data length: " + data.length);
                                            int length = data.length;

                                            // Create a buffer to hold the length and the data
                                            ByteBuf buffer = Unpooled.buffer(4 + length);
                                            buffer.writeInt(length);
                                            buffer.writeBytes(data);

                                            // Send the buffer to the server
                                            ctx.writeAndFlush(buffer);
                                        }

                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            cause.printStackTrace();
                                            ctx.close();
                                        }
                                    });
                        }
                    });
            waitingForSockFile(sock_path);
            // System.out.println("agent's sock is ready");
            Channel channel = bootstrap.connect(new DomainSocketAddress(sock_path.toAbsolutePath().toString())).sync()
                    .channel();

            channel.closeFuture().sync();

            // final FullHttpRequest request = new
            // DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            // "/services", Unpooled.EMPTY_BUFFER);
            // request.headers().set(HttpHeaderNames.HOST, "daemon");
            // channel.writeAndFlush(request);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            epollEventLoopGroup.shutdownGracefully();
        }
    }
}