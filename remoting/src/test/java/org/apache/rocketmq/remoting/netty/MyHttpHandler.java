package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.*;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;

import java.util.Objects;

public class MyHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private AsciiString contentType = HttpHeaderValues.TEXT_PLAIN;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        System.out.println("MyHttpHandler.channelRead0 " + msg.getClass().getName());
        DecoderResult decoderResult = msg.decoderResult();
        HttpHeaders reqHeaders = msg.headers();
        System.out.println(reqHeaders);
        DefaultFullHttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer("Welcome to netty's world!\nI'm your teacher!".getBytes(CharsetUtil.UTF_8)));
        HttpHeaders headers = defaultFullHttpResponse.headers();
        headers.add(HttpHeaderNames.CONTENT_TYPE, AsciiString.cached("text/plain; charset=utf-8"));
        headers.add(HttpHeaderNames.CONTENT_LENGTH, defaultFullHttpResponse.content().readableBytes());
        headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

        ctx.write(defaultFullHttpResponse);
        System.out.println("ctx write completed");

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        System.out.println("MyHttpHandler.channelReadComplete");
        super.channelReadComplete(ctx);
        ctx.flush();
        ctx.close();
        System.out.println("ctx flush completed");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("MyHttpHandler.exceptionCaught");
        if (Objects.nonNull(cause)) {
            cause.printStackTrace();
        }
        if (Objects.nonNull(ctx)) {
            ctx.close();
        }
    }
}
