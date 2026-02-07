//신규
package scouter.daemon;

import java.util.concurrent.atomic.AtomicReference;

public final class ConfigRef {
    private final AtomicReference<AppConfig> ref;

    public ConfigRef(AppConfig initial) {
        this.ref = new AtomicReference<>(initial);
    }

    public AppConfig get() {
        return ref.get();
    }

    public void set(AppConfig next) {
        ref.set(next);
    }
}