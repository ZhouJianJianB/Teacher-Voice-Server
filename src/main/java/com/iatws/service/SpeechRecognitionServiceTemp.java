package com.iatws.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.*;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

@Service
public class SpeechRecognitionServiceTemp extends WebSocketListener {

    public static final String hostUrl = "https://iat-api.xfyun.cn/v2/iat";
    public static final String appid = "d59b1c38";
    public static final String apiSecret = "OTBiZDU3NTJlZDcwMzQ3MjVkNmI0Zjlh";
    public static final String apiKey = "e174dab1b006d9a8072db0c52f6fa800";
    public static final int StatusFirstFrame = 0;
    public static final int StatusContinueFrame = 1;
    public static final int StatusLastFrame = 2;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS");

    private static Date dateBegin = new Date();
    private static Date dateEnd = new Date();

    private static final Gson json = new Gson();
    private Decoder decoder = new Decoder();

    public void recognize(byte[] audioData, Consumer<String> callback) throws Exception {
        String authUrl = getAuthUrl(hostUrl, apiKey, apiSecret);
        OkHttpClient client = new OkHttpClient.Builder().build();
        String url = authUrl.replace("http://", "ws://").replace("https://", "wss://");
        Request request = new Request.Builder().url(url).build();
        WebSocket webSocket = client.newWebSocket(request, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                super.onOpen(webSocket, response);
                new Thread(() -> {
                    // 连接成功，开始发送数据
                    int chunkSize = 8192;  // 每次发送的最大数据块大小，例如8192字节（8KB）
                    int offset = 0;        // 当前偏移量
                    int totalLength = audioData.length;  // 音频数据总长度

                    try {
                        while (offset < totalLength) {
                            // 计算剩余的音频数据
                            int remaining = totalLength - offset;
                            int len = Math.min(chunkSize, remaining); // 取出实际发送的长度
                            byte[] buffer = Arrays.copyOfRange(audioData, offset, offset + len); // 创建数据块

                            // 判断当前是首帧、中间帧还是最后一帧
                            int status;
                            if (offset == 0) {
                                status = StatusFirstFrame;  // 首帧
                            } else if (offset + len >= totalLength) {
                                status = StatusLastFrame;  // 最后一帧
                            } else {
                                status = StatusContinueFrame;  // 中间帧
                            }

                            // 构造发送的JSON数据
                            JsonObject frame = new JsonObject();
                            JsonObject common = new JsonObject();
                            JsonObject business = new JsonObject();
                            JsonObject data = new JsonObject();

                            // 填充 common 参数
                            common.addProperty("app_id", appid);

                            // 填充 business 参数
                            business.addProperty("language", "zh_cn");
                            business.addProperty("domain", "iat");
                            business.addProperty("accent", "mandarin");
                            business.addProperty("dwa", "wpgs");

                            // 填充 data 参数
                            data.addProperty("status", status);
                            data.addProperty("format", "audio/L16;rate=16000");
                            data.addProperty("encoding", "raw");
                            data.addProperty("audio", Base64.getEncoder().encodeToString(buffer));  // 将音频数据Base64编码

                            // 将 common、business、data 填充到 frame
                            frame.add("common", common);
                            frame.add("business", business);
                            frame.add("data", data);

                            // 发送到 WebSocket
                            webSocket.send(frame.toString());
                            // 增加偏移量
                            offset += len;

                            // 如果不是最后一帧，等待一定时间以模拟采样延迟
                            if (status != StatusLastFrame) {
                                Thread.sleep(40);  // 等待40毫秒
                            }
                        }

                        System.out.println("所有音频数据已发送完毕");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }).start();

            }
            @Override
            public void onMessage(WebSocket webSocket, String text) {
                super.onMessage(webSocket, text);
                //System.out.println(text);
                ResponseData resp = json.fromJson(text, ResponseData.class);
                if (resp != null) {
                    if (resp.getCode() != 0) {
                        System.out.println( "code=>" + resp.getCode() + " error=>" + resp.getMessage() + " sid=" + resp.getSid());
                        return;
                    }
                    if (resp.getData() != null) {
                        if (resp.getData().getResult() != null) {
                            Text te = resp.getData().getResult().getText();
                            //System.out.println(te.toString());
                            try {
                                decoder.decode(te);
//                                System.out.println("中间识别结果 ==》" + decoder.toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (resp.getData().getStatus() == 2) {
                            // todo  resp.data.status ==2 说明数据全部返回完毕，可以关闭连接，释放资源
//                            System.out.println("session end ");
                            dateEnd = new Date();
//                            System.out.println(sdf.format(dateBegin) + "开始");
//                            System.out.println(sdf.format(dateEnd) + "结束");
//                            System.out.println("耗时:" + (dateEnd.getTime() - dateBegin.getTime()) + "ms");]
                            if (decoder.toString() != null && !decoder.toString().equals("")) {
                                System.out.println(decoder.toString());
                                callback.accept(decoder.toString());
                            }
//                            System.out.println("本次识别sid ==》" + resp.getSid());
                            //decoder.discard();
                            //webSocket.close(1000, "");
                        } else {
                            // todo 根据返回的数据处理
                        }
                    }
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                super.onFailure(webSocket, t, response);
                try {
                    if (null != response) {
                        int code = response.code();
                        System.out.println("onFailure code:" + code);
                        System.out.println("onFailure body:" + response.body().string());
                        if (101 != code) {
                            System.out.println("connection failed");
                            System.exit(0);
                        }
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    private void sendFrame(WebSocket webSocket, byte[] buffer, int status) {
        JsonObject frame = new JsonObject();
        JsonObject data = new JsonObject();
        data.addProperty("status", status);
        data.addProperty("format", "audio/L16;rate=16000");
        data.addProperty("encoding", "raw");
        data.addProperty("audio", Base64.getEncoder().encodeToString(buffer));
        frame.add("data", data);
        webSocket.send(frame.toString());
    }

    public static String getAuthUrl(String hostUrl, String apiKey, String apiSecret) throws Exception {
        URL url = new URL(hostUrl);
        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        String date = format.format(new Date());
        StringBuilder builder = new StringBuilder("host: ").append(url.getHost()).append("\n").//
                append("date: ").append(date).append("\n").//
                append("GET ").append(url.getPath()).append(" HTTP/1.1");
        //System.out.println(builder);
        Charset charset = Charset.forName("UTF-8");
        Mac mac = Mac.getInstance("hmacsha256");
        SecretKeySpec spec = new SecretKeySpec(apiSecret.getBytes(charset), "hmacsha256");
        mac.init(spec);
        byte[] hexDigits = mac.doFinal(builder.toString().getBytes(charset));
        String sha = Base64.getEncoder().encodeToString(hexDigits);

        //System.out.println(sha);
        String authorization = String.format("api_key=\"%s\", algorithm=\"%s\", headers=\"%s\", signature=\"%s\"", apiKey, "hmac-sha256", "host date request-line", sha);
        //System.out.println(authorization);
        HttpUrl httpUrl = HttpUrl.parse("https://" + url.getHost() + url.getPath()).newBuilder().//
                addQueryParameter("authorization", Base64.getEncoder().encodeToString(authorization.getBytes(charset))).//
                addQueryParameter("date", date).//
                addQueryParameter("host", url.getHost()).//
                build();
        return httpUrl.toString();
    }



    public static class ResponseData {
        private int code;
        private String message;
        private String sid;
        private Data data;
        public int getCode() {
            return code;
        }
        public String getMessage() {
            return this.message;
        }
        public String getSid() {
            return sid;
        }
        public Data getData() {
            return data;
        }
    }
    public static class Data {
        private int status;
        private Result result;
        public int getStatus() {
            return status;
        }
        public Result getResult() {
            return result;
        }
    }
    public static class Result {
        int bg;
        int ed;
        String pgs;
        int[] rg;
        int sn;
        Ws[] ws;
        boolean ls;
        JsonObject vad;
        public Text getText() {
            Text text = new Text();
            StringBuilder sb = new StringBuilder();
            for (Ws ws : this.ws) {
                sb.append(ws.cw[0].w);
            }
            text.sn = this.sn;
            text.text = sb.toString();
            text.sn = this.sn;
            text.rg = this.rg;
            text.pgs = this.pgs;
            text.bg = this.bg;
            text.ed = this.ed;
            text.ls = this.ls;
            text.vad = this.vad==null ? null : this.vad;
            return text;
        }
    }
    public static class Ws {
        Cw[] cw;
        int bg;
        int ed;
    }
    public static class Cw {
        int sc;
        String w;
    }
    public static class Text {
        int sn;
        int bg;
        int ed;
        String text;
        String pgs;
        int[] rg;
        boolean deleted;
        boolean ls;
        JsonObject vad;
        @Override
        public String toString() {
            return "Text{" +
                    "bg=" + bg +
                    ", ed=" + ed +
                    ", ls=" + ls +
                    ", sn=" + sn +
                    ", text='" + text + '\'' +
                    ", pgs=" + pgs +
                    ", rg=" + Arrays.toString(rg) +
                    ", deleted=" + deleted +
                    ", vad=" + (vad==null ? "null" : vad.getAsJsonArray("ws").toString()) +
                    '}';
        }
    }
    public static class Decoder {
        private Text[] texts;
        private int defc = 10;
        public Decoder() {
            this.texts = new Text[this.defc];
        }
        public synchronized void decode(Text text) {
            if (text.sn >= this.defc) {
                this.resize();
            }
            if ("rpl".equals(text.pgs)) {
                for (int i = text.rg[0]; i <= text.rg[1]; i++) {
                    this.texts[i].deleted = true;
                }
            }
            this.texts[text.sn] = text;
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Text t : this.texts) {
                if (t != null && !t.deleted) {
                    sb.append(t.text);
                }
            }
            return sb.toString();
        }
        public void resize() {
            int oc = this.defc;
            this.defc <<= 1;
            Text[] old = this.texts;
            this.texts = new Text[this.defc];
            for (int i = 0; i < oc; i++) {
                this.texts[i] = old[i];
            }
        }
        public void discard(){
            for(int i=0;i<this.texts.length;i++){
                this.texts[i]= null;
            }
        }
    }
}

