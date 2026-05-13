package com.iatws.socket;

import com.iatws.service.SpeechRecognitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;
import org.springframework.web.util.WebAppRootListener;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.websocket.OnMessage;
import java.io.IOException;
import java.nio.ByteBuffer;

@Component
public class SpeechWebSocketHandler extends BinaryWebSocketHandler implements ServletContextInitializer {

    private static final Logger log = LoggerFactory.getLogger(SpeechWebSocketHandler.class);
    @Autowired
    SpeechRecognitionService speechRecognitionService;

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        ByteBuffer byteBuffer = message.getPayload();
        byte[] audioData = new byte[byteBuffer.remaining()];
        byteBuffer.get(audioData);

        // 调用讯飞的识别服务并返回结果
        speechRecognitionService.recognize(audioData, result -> {
            try {
                System.out.println("获取结果："+ result);
                session.sendMessage(new TextMessage(result)); // 将识别结果返回给前端
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
//        System.out.println("WebSocket 连接已建立: " + session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
//        System.out.println("WebSocket 连接已关闭: " + session.getId());
    }

    /**
     * 配置websocket文件接受的文件最大容量
     * @param servletContext    context域对象
     * @throws ServletException 抛出异常
     */
    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
        servletContext.addListener(WebAppRootListener.class);
        servletContext.setInitParameter("org.apache.tomcat.websocket.textBufferSize","51200000");
        servletContext.setInitParameter("org.apache.tomcat.websocket.binaryBufferSize","51200000");
    }
}


