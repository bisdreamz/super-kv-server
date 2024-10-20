package org.reset.replication.discovery;

import java.util.Objects;

public class Peer {

    public static enum State {
        DOWN_MANAGER,
        DOWN_PROBE,
        UP
    }

    public Peer(String host, boolean self) {
        this.host = host;
        this.self = self;
        this.state = State.UP; // Default state
    }

    public Peer(Peer other) {
        this.host = other.host;
        this.self = other.self;
        this.state = other.state;
    }

    private final boolean self;
    private final String host;
    private State state;

    public boolean isSelf() {
        return self;
    }

    public String getHost() {
        return host;
    }

    public State getState() {
        return state;
    }

    public Peer setState(State state) {
        this.state = state;
        return this;
    }

    @Override
    public String toString() {
        return "Peer{" +
                "host='" + host + '\'' +
                ", self=" + self +
                ", state=" + state +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Peer peer = (Peer) o;

        return Objects.equals(host, peer.host);
    }

    @Override
    public int hashCode() {
        return host != null ? host.hashCode() : 0;
    }
}