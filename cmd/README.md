# Usage
  
### Running a local cluster  

```go
cmd -idx 1   -peer 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315   -port 12316   -workerport 12315  -w ./wal1
cmd -idx 2   -peer 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315   -port 22316   -workerport 22315  -w ./wal2
cmd -idx 3   -peer 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315   -port 32316   -workerport 32315  -w ./wal3
```

### Put and get value  

```go
curl -L http://127.0.0.1:12380/my-key -XPUT -d bar
curl -L http://127.0.0.1:32380/my-key
```

### add a new node to cluster  

```go
cmd -idx 4   -peer 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315,127.0.0.1:42315   -port 42316   -workerport 42315  -w ./wal4 -join
```
 




