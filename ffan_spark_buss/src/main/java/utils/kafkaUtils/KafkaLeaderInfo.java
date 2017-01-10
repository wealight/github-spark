package utils.kafkaUtils;

/**
 * Created by weishuxiao on 16/8/26.
 */
import java.net.URI;

public class KafkaLeaderInfo {

    private URI uri;
    private int leaderId;

    public KafkaLeaderInfo(URI uri, int leaderId) {
        this.uri = uri;
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public URI getUri() {
        return uri;
    }

    @Override
    public int hashCode() {
        return this.uri.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this.hashCode() == obj.hashCode();
    }
}