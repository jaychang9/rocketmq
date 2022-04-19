package org.apache.rocketmq.remoting.netty;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseDecoder;

import java.io.IOException;

public class HttpServer {
    private int port;

    public HttpServer(int port) {
        this.port = port;
    }


    public void start() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workGroup = new NioEventLoopGroup();
        b.group(bossGroup,workGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        System.out.println("HttpServer.initChannel " + ch);
                        ch.pipeline().addLast("decoder", new HttpRequestDecoder())
                                .addLast("encoder", new HttpResponseDecoder())
                                .addLast("aggregator", new HttpObjectAggregator(512 * 1024))
                                .addLast("handler",new MyHttpHandler());

                    }
                });

        b.bind(port).sync();
    }

    public static void main(String[] args) throws InterruptedException {
        HttpServer httpServer = new HttpServer(8888);
        httpServer.start();
    }
}
