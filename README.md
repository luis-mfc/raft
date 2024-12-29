# Raft protocol implementation
This is a example implementation of the raft consensus protocol:
- https://raft.github.io/raft.pdf
- https://raft.github.io/

## How to run
1. Compile the project:
```shell
javac *.java
```
2. Configure the number of instances (list of local ports): `config.conf`
3. Configure general settings: `config.properties`
4. Run the instances:
```shell
java FollowerMain <follower number, e.g., 0, 1, ...>
```
1. Run the client application:
```shell
java Client [serverNumber] <put|get|del|cas|list> <numberOfRequests>
```

## Docker
1. Run Raft cluster:
```shell
docker compose up --build
```
2. Manual Test:
```shell
docker run --network host -it --rm --entrypoint bash $(docker build -q .)
java Client 0 list 1
java Client 0 put 1
java Client 0 get 1
```
