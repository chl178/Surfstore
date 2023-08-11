# Introduction
In the pursuit of designing a fault-tolerant system, we will be creating a RaftSurfstoreServer. This server, drawing inspiration from the MetaStore in project 4, will be advanced in its capabilities to withstand errors and continue its operation. This document will discuss its design, communication mechanisms, and the testing methodology to ensure its reliability.

## Design

### Overview of RaftSurfstoreServer
- The RaftSurfstoreServer is responsible for functioning as a fault-tolerant MetaStore.
- Communication between the RaftSurfstoreServers is facilitated through GRPC.
- While each server will have knowledge about other potential servers from the configuration file, there won't be provisions for new servers to join the cluster dynamically. However, the existing servers possess the capability to "crash" using the Crash API.
- Leaders within the system will be determined via the SetLeader API call, eliminating the need for elections.

### Protocol Dynamics
- When a leader server queries a majority quorum of nodes and gets an affirmative response, it will then convey the accurate answer back to the client.
- The system remains accessible to the clients as long as over half of the nodes are operational and not in a crashed state. Conversely, when the majority of nodes crash, client requests are put on hold, only resuming when most of the nodes have been restored.
- Any interaction initiated by clients with a non-leader should result in an error prompt, after which the client should attempt to identify and connect with the leader.

## ChaosMonkey: Testing Reliability

To put our implementation to the test, we'll employ the **RaftTestingInterface**. This interface has been endowed with:
- Three functions dedicated to 'chaos' testing.
- One function to fetch the internal state.

Rather than causing an actual program crash, when a server receives a Crash() call, it is designed to enter a simulated "crashed" state. During this state, if it encounters any AppendEntries or similar calls, the server should return an error, keeping its internal state unchanged. Moreover, a client's attempt to engage with a crashed node will be met with an error, steering the client to look for the genuine leader.

For evaluation purposes, our autograding system will deploy the Crash/Restore/GetInternalState methods. It's crucial to run your own tests invoking these methods to validate the robustness and adherence of the RAFT properties in your solution.

## Usage
1. Run your server using this:
```shell
go run cmd/SurfstoreServerExec/main.go -s <service> -p <port> -l -d (BlockStoreAddr*)
```
Here, `service` should be one of three values: meta, block, or both. This is used to specify the service provided by the server. `port` defines the port number that the server listens to (default=8080). `-l` configures the server to only listen on localhost. `-d` configures the server to output log statements. Lastly, (BlockStoreAddr\*) is the BlockStore address that the server is configured with. If `service=both` then the BlockStoreAddr should be the `ip:port` of this server.

2. Run your client using this:
```shell
go run cmd/SurfstoreClientExec/main.go -d <meta_addr:port> <base_dir> <block_size>
```

## Examples:
```shell
go run cmd/SurfstoreServerExec/main.go -s both -p 8081 -l localhost:8081
```
This starts a server that listens only to localhost on port 8081 and services both the BlockStore and MetaStore interface.

```shell
Run the commands below on separate terminals (or nodes)
> go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l
> go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081
```
The first line starts a server that services only the BlockStore interface and listens only to localhost on port 8081. The second line starts a server that services only the MetaStore interface, listens only to localhost on port 8080, and references the BlockStore we created as the underlying BlockStore. (Note: if these are on separate nodes, then you should use the public ip address and remove `-l`)

3. From a new terminal (or a new node), run the client using the script provided in the starter code (if using a new node, build using step 1 first). Use a base directory with some files in it.
```shell
> mkdir dataA
> cp ~/pic.jpg dataA/ 
> go run cmd/SurfstoreClientExec/main.go server_addr:port dataA 4096
```
This would sync pic.jpg to the server hosted on `server_addr:port`, using `dataA` as the base directory, with a block size of 4096 bytes.

4. From another terminal (or a new node), run the client to sync with the server. (if using a new node, build using step 1 first)
```shell
> mkdir dataB
> go run cmd/SurfstoreClientExec/main.go server_addr:port dataB 4096
> ls dataB/
pic.jpg index.txt
```
We observe that pic.jpg has been synced to this client.

## Makefile
We also provide a make file for you to run the BlockStore and MetaStore servers.
1. Run both BlockStore and MetaStore servers (**listens to localhost on port 8081**):
```shell
make run-both
```

2. Run BlockStore server (**listens to localhost on port 8081**):
```shell
make run-blockstore
```

3. Run MetaStore server (**listens to localhost on port 8080**):
```shell
make run-metastore
```

## Testing 
On gradescope, only a subset of test cases will be visible, so we highly encourage you to come up with different scenarios like the one described above. You can then match the outcome of your implementation to the expected output based on the theory provided in the writeup.
