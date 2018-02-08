# nentropy

[![Build Status](https://travis-ci.org/journeymidnight/nentropy.svg?branch=master)](https://travis-ci.org/journeymidnight/nentropy)
[![license](https://img.shields.io/github/license/journeymidnight/nentropy.svg)](https://github.com/journeymidnight/nentropy/blob/master/LICENSE)


### Build


How to build?

cd $NENTROPY_DIR  
make  

Start mon:
```shell
./mon -idx 1 -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315 -memberBindPort 7946 -advertiseAddr 127.0.0.1:12315  
./mon -idx 2 -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315 -joinMemberAddr 127.0.0.1:7946 -advertiseAddr 127.0.0.1:22315
```


Add osd:
```shell
./tools -t osd -c add -id 1 -server_addr 127.0.0.1:12315  
./tools -t osd -c add -id 2 -server_addr 127.0.0.1:12315  
./tools -t osd -c add -id 3 -server_addr 127.0.0.1:12315  
```

Start osd:
```shell
./osd -nodeID 1  -advertiseAddr 127.0.0.1:8898 -joinMemberAddr 127.0.0.1:7946   
./osd -nodeID 2  -advertiseAddr 127.0.0.1:8898 -joinMemberAddr 127.0.0.1:7946  
./osd -nodeID 3  -advertiseAddr 127.0.0.1:8898 -joinMemberAddr 127.0.0.1:7946    
```

Create pool and pg:
```shell
./tools -server_addr 127.0.0.1:12315 -t pool -c create -p test -pg_number 3    
```

Put file and get file:
```shell
./tools -p test  -t object -c put -oid 2M -f ./2M -server_addr 127.0.0.1:12315
./tools -p test  -t object -c get -oid 2M -f ./2M.res -server_addr 127.0.0.1:12315
diff 2M  2M.res
```
