package org.reset.server;

import com.nimbus.proto.protocol.HeaderProtocol;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class NettyProtoServer {

    private final int port;
    private final Function<ByteBuf, ByteBuf> requestHandler;

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public NettyProtoServer(int port, Function<ByteBuf, ByteBuf> requestHandler) {
        this.port = port;
        this.requestHandler = requestHandler;
    }

    public CompletableFuture<Void> start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 8192)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF, 65536)
                .option(ChannelOption.SO_SNDBUF, 65536)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel socketChannel) {
                        socketChannel.pipeline().addLast(new ProtoRequestDecoder());
                        socketChannel.pipeline().addLast(new RequestHandler(requestHandler));
                    }
                });

        CompletableFuture<Void> future = new CompletableFuture<>();
        this.channelFuture = serverBootstrap.bind(port).addListener((ChannelFutureListener) bindFuture -> {
            if (bindFuture.isSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(bindFuture.cause());
            }
        });

        return future;
    }

    public CompletableFuture<Void> shutdown() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (bossGroup != null)
            bossGroup.shutdownGracefully();

        if (workerGroup != null)
            workerGroup.shutdownGracefully();

        channelFuture.channel().closeFuture().addListener((ChannelFutureListener) closeFuture -> {
            if (closeFuture.isSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(closeFuture.cause());
            }
        });

        return future;
    }

    /**
     * Decoder that reads the first 4 bytes as the message length
     * and waits until the full message has arrived before passing
     * the complete message to the next handler.
     */
    private static class ProtoRequestDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < HeaderProtocol.HDR_TOTAL_LEN.sizeBytes())
                return;

            int length = in.getInt(in.readerIndex());

            if (in.readableBytes() < length)
                return;

            ByteBuf message = in.readSlice(length);
            out.add(message.retain());
        }
    }

    public class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final Function<ByteBuf, ByteBuf> requestHandler;

        public RequestHandler(Function<ByteBuf, ByteBuf> requestHandler) {
            this.requestHandler = requestHandler;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            ByteBuf response = null;
            try {
                // Process the incoming message and generate a response
                response = requestHandler.apply(msg);

                // If the response is the same as the incoming message, retain it to prevent it from being released
                if (response == msg) {
                    msg.retain();
                }

                // Write and flush the response to the client
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                // In case of an exception, write an error message back to the client and close the connection
                ByteBuf errorBuf = ctx.alloc().buffer();
                errorBuf.writeBytes(("Error: " + e.getMessage()).getBytes());
                ctx.writeAndFlush(errorBuf);
                ctx.close();

                // Log the error for debugging purposes
                System.err.println("Error: " + e.getMessage());
            }
            // No need for a finally block to release 'msg' because Netty 4.2's SimpleChannelInboundHandler releases it automatically
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // Handle unexpected exceptions to prevent the application from crashing
            cause.printStackTrace();
            ctx.close();
        }
    }
}