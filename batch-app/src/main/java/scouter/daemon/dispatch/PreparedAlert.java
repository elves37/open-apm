package scouter.daemon.dispatch;

public class PreparedAlert {
    public final String toName;
    public final String title;
    public final String message;
    public final String text;
    public final long occurredAt;
    public final String severity;

    public PreparedAlert(String toName, String title, String message, String text, long occurredAt, String severity) {
        this.toName = toName;
        this.title = title;
        this.message = message;
        this.text = text;
        this.occurredAt = occurredAt;
        this.severity = severity;
    }
}