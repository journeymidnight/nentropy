#!/bin/sh
./mon -idx 1   -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315    -monPort 12315  -w ./wal1  -memberBindPort 7946 &
./mon -idx 2   -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315    -monPort 22315  -w ./wal2   -memberBindPort 7846 &
./mon -idx 3   -mons 127.0.0.1:12315,127.0.0.1:22315,127.0.0.1:32315    -monPort 32315  -w ./wal3   -memberBindPort 7746 &

