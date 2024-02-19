package csx55.overlay;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class OverlayCreator {
    public void constructRing (Set<OverlayNode> overlayNodes) {
        setupTopology(overlayNodes);
    }

    private void setupTopology (Set<OverlayNode> overlayNodes) {
        int idx = 0;
        OverlayNode [] overlayNodesArray = overlayNodes.toArray(new OverlayNode[0]);
        for (OverlayNode node: overlayNodesArray) {
            node.addNeighbors(overlayNodesArray[(idx + 1) % overlayNodes.size()]);
            idx++;
        }
    }
}
