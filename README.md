# Load-Balancing

Load balancer with custom threadpool implementation which automatically distributes task between various nodes in a ring topology. The algorithm being used is one based on mean convergence where the mean of the total tasks in the network is considered the target load and the adjustments are made in the nodes until the target is attained. 

The load adjustment begins at a randomly designated origin node with a token. This origin node with the token is responsible for initiating the load balancing operation until it achieves the target, computed after sending a message to compute total load load across the ring. If the token node has load more than the mean, it will look for the node with the minimum load in the network and push tasks `(n = current_load - target)` to this light load. If the token node has less load than target, it will pull `(target - current_load)` tasks from the node which has the heaviest load. This will continue until the token node attains equilibrium (meets the target value)

If the token node is somehow already balanced (at the equilibrium) before load-balancing, then the token should be passed on to another random node not at equilibrium. This is not implemented in this solution yet.

This project was done as part of Programming Assignment 2 for the CS-555-Distributed Systems class taught at Colorado state. Spring 2024.
