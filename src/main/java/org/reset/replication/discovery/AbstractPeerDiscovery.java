package org.reset.replication.discovery;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public abstract class AbstractPeerDiscovery {

    private Consumer<Peer> onPeerChanged;
    /**
     * Initialize peer management service and load initial list of known peers
     * @return Future which resolves upon success
     */
    public abstract CompletableFuture<List<Peer>> init();

    public abstract List<Peer> getPeers();

    /**
     * Disconnects from any related peer watchers or services, and should
     * remove the self node from the peer list if applicable
     * @return Future which resolves upon completion
     */
    public abstract CompletableFuture<Void> shutdown();

    /**
     * Callback method which should handle events when the peers list has changed.
     * The peer provided as an argument will be the peer whose
     * state has changed (come up or gone down)
     * @param consumer A method which should handle this state change,
     *                 such as kicking off a replication or rebalance
     */
    public final void onPeerChanged(Consumer<Peer> consumer) {
        this.onPeerChanged = consumer;
    }

}
