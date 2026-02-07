package hi.internal.process.outbound;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;

public class OutboundProcess {
    private final HttpClient client;
    private String userId;

    public OutboundProcess() {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public HttpResponse<String> call(String url, String ifId, String reqJson) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Accept", "application/json")
                .header("user-id", this.userId)
                .header("interface-id", ifId);

        HttpRequest req = b.POST(HttpRequest.BodyPublishers.ofString(reqJson)).build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}