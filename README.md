# Raft protocol implementation
## How to run
1. Compile the project:
```shell
javac *.java
```
2. Configure the number of followers: `config.conf`
3. Configure general settings: `config.properties`
4. Run the follower instances:
```shell
java FollowerMain <follower number, e.g., 0, 1, ...>
```
5. Run the client application:
```shell
java Client [serverNumber] <put|get|del|cas|list> <numberOfRequests>
```
