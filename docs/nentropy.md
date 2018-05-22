# Nentropy 设计

小文件的存储一直是分布式存储设计的难点, 这里的小文件
它有2个主要问题需要解决:

1. 如何高效率的在本地存储小文件?
2. 如何在分布式系统中平衡延迟和一致性?

## 本地存储小文件

优化存储小文件的思路大体就是和并小IO, 更新小文件也不再in-place更新, 而是永远一直写新的位置,
这2种方式都可以减少随机IO. 这个想法最早出现在文件系统的研究中, 参考论文[The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)或者这个[这个Slide](http://www.eecs.harvard.edu/~cs161/notes/lfs.pdf)也有很多介绍.

和此对应的在userspace里面的实现就是facebook存储图片的论文:[Finding a needle in Haystack: Facebook’s photo storage](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf) 或者
[bitcask的论文](http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf)


Netropy选择了根据论文[WiscKey: Separating Keys from Values
in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), 开发的[badge-io](https://github.com/dgraph-io/badger), 这个存储引擎把实际数据存储在VLOG中, 而对应的索引结构持久化在类似于levelDB的KV数据库中, 由于
KV数据库只存储实际小文件的索引, 这个KV数据库非常小, 也可以极大地减少写放大的压力, 从badge[官方对比测试数据](https://blog.dgraph.io/post/badger-lmdb-boltdb/)看, 在badge存储的value大于1KB时, badge的读写性能大约是[boltDB](https://github.com/boltdb/bolt)的4倍到100倍. badge-io的架构刚好适合存储小文件, 如1KB ~ 2MB左右的图片.  

小文件存储的底层引擎不同于
一般Key-Value存储引擎, 它的value比较大(1KB ~ 2MB). 而rocksdb等kv数据库的典型应用的value通常不到几百字节. 之前笔者压测过用Rocksdb存储图片, 由于写放大的问题, 效果不如预期. 而badge-io反而在大value下表现更好.

此外, badge-io也不同于[bitcask](http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf), bitcask采用HashTable存储所有索引. 所有索引都放在内存, 对内存空间要求也高. badge-io相比bitcask也更节省内存.

基于以上2个原因, Nentropy选择badge-io作为底层存储引擎.


## Raft算法保证数据一致性

分布式性一致性通过[Raft协议](raft.github.io)解决, 由于raft leader在收到多数成功后就返回, 相比与其他强一致分布式系统, 写入延迟更小.
Raft算法的实现采用[etcd的raft实现](https://github.com/coreos/etcd/tree/master/raft), 在read策略上, 我们也不需要严格的Linearizability
Read. 选择Lease read, 即使有可能存在时钟漂移, 出现stale read, 在对象存储的应用中也完全可以接受.


# 架构

## Object映射

## Pg的分配

## Member List 管理

## 加减OSD

## 修改badge-io, 支持Nentropy的snapshot

## Multiraft

## Truncate Log

# 与其他分布式系统对比

# 性能测试