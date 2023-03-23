# fly-io-distributed-challenge
My attempt at the fly.io distributed challenge

1. Fairly straightforward, didn't include in the repo
2. Fairly straightforward, got away with uuids
3. Broadcast
    - First part was a simple broadcast.
    - Second part was to just naively send messages to everyone. 
    - Third part was to send messages but do them with retries. 
    - Fourth part, I just had a naive topology with one master node that everyone talked to and received messages from (not really the best in practice). 
    - Last part, I used message batching in concordance with retries and the naive topology
