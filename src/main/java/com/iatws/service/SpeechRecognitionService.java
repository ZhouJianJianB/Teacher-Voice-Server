package com.iatws.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.iatws.rtasr.DraftWithOrigin;
import com.iatws.util.EncryptUtil;
import io.netty.util.internal.StringUtil;
import okhttp3.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

@Service
public class SpeechRecognitionService extends WebSocketListener {


    // appid
    private static final String APPID = "d59b1c38";

    // appid对应的secret_key
    private static final String SECRET_KEY = "33cfa461486872854fe78103f47f3705";

    // 请求地址
    private static final String HOST = "rtasr.xfyun.cn/v1/ws";

    private static final String BASE_URL = "ws://" + HOST;

    private static final String ORIGIN = "http://" + HOST;

    // 每次发送的数据大小 1280 字节
    private static final int CHUNCKED_SIZE = 1280;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS");


    public void recognize(byte[] audioData, Consumer<String> callback) throws Exception {
        URI url = new URI(BASE_URL + getHandShakeParams(APPID, SECRET_KEY));
        DraftWithOrigin draft = new DraftWithOrigin(ORIGIN);
        CountDownLatch handshakeSuccess = new CountDownLatch(1);
        CountDownLatch connectClose = new CountDownLatch(1);
        SpeechRecognitionService.MyWebSocketClient client = new SpeechRecognitionService.MyWebSocketClient(url, draft, handshakeSuccess, connectClose, callback);

        client.connect();
        while (true) {
            while (!client.getReadyState().equals(org.java_websocket.WebSocket.READYSTATE.OPEN)) {
                Thread.sleep(40);
            }

            // 等待握手成功
            handshakeSuccess.await();

            // 使用 ByteArrayInputStream 从 byte[] 读取数据
            try (ByteArrayInputStream bais = new ByteArrayInputStream(audioData)) {
                byte[] bytes = new byte[CHUNCKED_SIZE];
                int len;
                long lastTs = 0;

                while ((len = bais.read(bytes)) != -1) {
                    if (len < CHUNCKED_SIZE) {
                        send(client, Arrays.copyOfRange(bytes, 0, len));
                        break;
                    }

                    long curTs = System.currentTimeMillis();
                    if (lastTs == 0) {
                        lastTs = curTs;
                    } else {
                        long s = curTs - lastTs;
                        if (s < 40) {
                            System.out.println("error time interval: " + s + " ms");
                        }
                    }
                    send(client, bytes);
                    // 每隔40毫秒发送一次数据
                    Thread.sleep(40);
                }

                // 发送结束标识
                send(client, "{\"end\": true}".getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }

            // 等待连接关闭
            connectClose.await();
            break;
        }

    }

    public static String getHandShakeParams(String appId, String secretKey) {
        String ts = System.currentTimeMillis()/1000 + "";
        String signa = "";
        try {
            signa = EncryptUtil.HmacSHA1Encrypt(EncryptUtil.MD5(appId + ts), secretKey);
            return "?appid=" + appId + "&pd=edu&punc=0&ts=" + ts + "&signa=" + URLEncoder.encode(signa, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    public static void send(WebSocketClient client, byte[] bytes) {
        if (client.isClosed()) {
            throw new RuntimeException("client connect closed!");
        }

        client.send(bytes);
    }

    public static String getCurrentTimeStr() {
        return sdf.format(new Date());
    }

    public static class MyWebSocketClient extends WebSocketClient {

        private CountDownLatch handshakeSuccess;
        private CountDownLatch connectClose;
        private Consumer<String> callback; // 添加回调

        public MyWebSocketClient(URI serverUri, Draft protocolDraft, CountDownLatch handshakeSuccess, CountDownLatch connectClose, Consumer<String> callback) {
            super(serverUri, protocolDraft);
            this.handshakeSuccess = handshakeSuccess;
            this.connectClose = connectClose;
            this.callback = callback; // 初始化回调
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
        }

        @Override
        public void onMessage(String msg) {
            JSONObject msgObj = JSON.parseObject(msg);
            String action = msgObj.getString("action");
            if (Objects.equals("started", action)) {
                // 握手成功
                handshakeSuccess.countDown();
            } else if (Objects.equals("result", action)) {
                // 转写结果
                String result = getContent(msgObj.getString("data"));
                if (callback != null) {
                    callback.accept(result); // 返回结果
                }
            } else if (Objects.equals("error", action)) {
                // 连接发生错误
                System.out.println("Error: " + msg);
                System.exit(0);
            }
        }

        @Override
        public void onError(Exception e) {
            System.out.println(getCurrentTimeStr() + "\t连接发生错误：" + e.getMessage() + ", " + new Date());
            e.printStackTrace();
            System.exit(0);
        }

        @Override
        public void onClose(int arg0, String arg1, boolean arg2) {
            connectClose.countDown();
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            try {
                System.out.println(getCurrentTimeStr() + "\t服务端返回：" + new String(bytes.array(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    // 把转写结果解析为句子
    public static String getContent(String message) {
        StringBuffer resultBuilder = new StringBuffer();
        try {
            JSONObject messageObj = JSON.parseObject(message);
            JSONObject cn = messageObj.getJSONObject("cn");
            JSONObject st = cn.getJSONObject("st");
            JSONArray rtArr = st.getJSONArray("rt");
            for (int i = 0; i < rtArr.size(); i++) {
                JSONObject rtArrObj = rtArr.getJSONObject(i);
                JSONArray wsArr = rtArrObj.getJSONArray("ws");
                for (int j = 0; j < wsArr.size(); j++) {
                    JSONObject wsArrObj = wsArr.getJSONObject(j);
                    JSONArray cwArr = wsArrObj.getJSONArray("cw");
                    for (int k = 0; k < cwArr.size(); k++) {
                        JSONObject cwArrObj = cwArr.getJSONObject(k);
                        String wStr = cwArrObj.getString("w");
                        resultBuilder.append(wStr);
                    }
                }
            }
        } catch (Exception e) {
            return message;
        }

        return resultBuilder.toString();
    }

}


