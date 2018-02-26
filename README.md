# nentropy

[![Build Status](https://travis-ci.org/journeymidnight/nentropy.svg?branch=master)](https://travis-ci.org/journeymidnight/nentropy)
[![license](https://img.shields.io/github/license/journeymidnight/nentropy.svg)](https://github.com/journeymidnight/nentropy/blob/master/LICENSE)


### Build


How to build?

cd $NENTROPY_DIR  
make  

Start mon:
```shell
nentropy_monitor -idx 1 -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315 -mberBindPort 7946 -advertiseAddr 127.0.0.1:12315 
nentropy_monitor -idx 2 -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315 -joinMemberAddr27.0.0.1:7946 -advertiseAddr 127.0.0.1:22315
```


Add osd:
```shell
./build/bin/nentropy_admin -t osd -c add -id 1 -server_addr 127.0.0.1:12315
./build/bin/nentropy_admin -t osd -c add -id 2 -server_addr 127.0.0.1:12315
./build/bin/nentropy_admin -t osd -c add -id 3 -server_addr 127.0.0.1:12315 
```

Start osd:
```shell
./build/bin/nentropy_osd -nodeID 1 -joinMemberAddr 127.0.0.1:7946   
./build/bin/nentropy_osd -nodeID 2 -joinMemberAddr 127.0.0.1:7946
./build/bin/nentropy_osd -nodeID 3 -joinMemberAddr 127.0.0.1:7946   
```

Create pool and pg:
```shell
./build/bin/nentropy_admin -t pool -c create -p test -pg_number 8 -server_addr 127.0.0.1:12315  
```

Put file and get file:
```shell
./build/bin/nentropy_admin -p test -t object -c put -oid 111 -f ./2M -server_addr 127.0.0.1:12315
./build/bin/nentropy_admin -p test -t object -c get -oid 111 -f ./2M.replica -server_addr 127.0.0.1:12315
diff 2M  2M.res
```
