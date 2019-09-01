# Teamo

[![Author](https://img.shields.io/badge/Author-owtotwo-yellow.svg)](https://github.com/owtotwo)
[![MIT license](https://img.shields.io/badge/License-Apache--2.0-brightgreen.svg)](https://img.shields.io/badge/License-Apache--2.0-brightgreen.svg)
![Release](https://img.shields.io/badge/Release-0.1.0-blue.svg)

_`暂时只有中文文档`_



## Why I do this

市面上常见的各种图数据库过于笨重，在对于中小型项目的使用场景的适应上显得并不太友好。

此项目的定位于SQLite类似，对于高并发、高容错性、海量数据等支持较弱，但相较于Neo4j、Titan、HugeGraph等大型图数据库，此项目更灵活、轻量，不需要过多的配置，单个文件即整个项目的模块，直接引入并使用即可。

对于SQLite的应用场景，具体可以参考[SQLite官网描述的使用场景](https://www.sqlite.org/whentouse.html)。



## Feature

- 支持多个底层关系型数据库（现阶段只有两个）
- SQLite3 作为底层引擎时，生成一百万个点，平均出度为十五（一千五百万条边）的稀疏图耗时21min 32sec。
- 目前基本支持最高上亿级点边的图。



## Requirement

#### 以 SQLite3 为引擎

- 目前只支持 Python3.7 版本。（只需这个，因为此版本自带 [sqlite3](https://docs.python.org/3/library/sqlite3.html) ）
- 已测试 **Windows 10 v1809** and **Ubuntu 18.04** （其他环境未测试，但只要符合上面一点，基本都能支持）

#### 以 MySQL 为引擎

- **MySQL 5.7+** or **MariaDB 10.2+**
- 只测试了 **Windows 10 v1809**

_注：因目前项目中基本不存在平台相关性的代码，所以应该大部分平台都可以使用。_



## Install

将项目中的`teamo.py`下载下来，并放到将要使用的目录中即可。



## Usage

因为python3自带SQLite3

以SQLite3为例：

``` python
# 引入符合python的PEP-249所定义的DB-API 2.0协议的数据库模块
import sqlite3
# 引入此项目中唯一的接口
from teamo import Teamo

# 创建sqlite3数据库的连接（此处使用内存作为存储空间）
conn = sqlite3.connect(':memory:')

# 创建基于数据库的抽象图
graph = Teamo(conn, db='sqlite3')

# 从携有数据库信息的图实例中构建空图
graph.init()

# 创造一个遍历对象（作为查询修改图使用）
g = graph.traversal()

# 创建点
tom = g.addV().label('cat').data('{"name":"Tom"}').id()
jerry = g.addV().label('mouse').data('{"name":"Jerry"}').id()

# 创建边
g.addE(tom, jerry).label('love').data('{"weight":"0.5"}')

# 查询tom喜欢对象的名字
lover = g.V().hasLabel('cat').out('love').values('name')

# 输出结果
print('Tom love {}.'.format(lover))

# 关闭数据库
conn.close()
```

当然，这只是个简单的例子。

MySQL也类似（使用[pymysql](https://github.com/PyMySQL/PyMySQL)数据库模块）

具体样例请参考 [sample-sqlite3.py](./sample-sqlite3.py) 与 [sample-mysql.py](./sample-mysql.py) 。



## Learn More

- [Gremlin Language](https://tinkerpop.apache.org/gremlin.html)
- [Gremlin Modern graph](http://tinkerpop.apache.org/docs/current/images/tinkerpop-modern.png)
- [Graph Database](https://en.wikipedia.org/wiki/Graph_database)
- [PEP-249](https://www.python.org/dev/peps/pep-0249/)
- [Comment Syntax](https://dev.mysql.com/doc/refman/5.7/en/comments.html) and https://stackoverflow.com/a/41028314
- ...



## Benchmark

参考 [HugeGraph文档中的Performance对比](https://hugegraph.github.io/hugegraph-doc/performance/hugegraph-benchmark-0.5.6.html)

#### 关于的数据集

测试使用人造数据和真实数据

- MIW、SIW和QW使用SNAP数据集
  - [Enron Dataset](http://snap.stanford.edu/data/email-Enron.html)
  - [Amazon dataset](http://snap.stanford.edu/data/amazon0601.html)
  - [Youtube dataset](http://snap.stanford.edu/data/com-Youtube.html)
  - [LiveJournal dataset](http://snap.stanford.edu/data/com-LiveJournal.html)

测试用的数据规模

| 名称                    | vertex数目 | edge数目  | 文件大小 |
| :---------------------- | :--------- | :-------- | :------- |
| email-enron.txt         | 36,691     | 367,661   | 4MB      |
| com-youtube.ungraph.txt | 1,157,806  | 2,987,624 | 38.7MB   |
| amazon0601.txt          | 403,393    | 3,387,388 | 47.9MB   |
| com-lj.ungraph.txt      | 3997961    | 34681189  | 479MB    |

#### 关于测试环境

环境一

| CPU                                          | Memory | 网卡      | 磁盘      |
| :------------------------------------------- | :----- | :-------- | :-------- |
| 48 Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz | 128G   | 10000Mbps | 750GB SSD |

环境二

| CPU                                      | Memory | 磁盘  |
| :--------------------------------------- | :----- | :---- |
| 1 AMD Ryzen 7 2700X Eight-Core Processor | 16G    | 500GB |

- HugeGraph版本：0.5.6，RestServer和Gremlin Server和backends都在同一台服务器上 
  - RocksDB版本：rocksdbjni-5.8.6
  - 测试环境为**环境一**
- Titan版本：0.5.4, 使用thrift+Cassandra模式
  - Cassandra版本：cassandra-3.10，commitlog和data共用SSD
  - 测试环境为**环境一**
- Neo4j版本：2.0.1
  - 测试环境为**环境一**
- Teamo版本：0.1.0，使用SQLite3，单机单线程模式
  - 测试环境为**环境二**

#### 测试结果

##### Batch插入性能

| Backend   | email-enron(30w) | amazon0601(300w) | com-youtube.ungraph(300w) | com-lj.ungraph(3000w) |
| :-------- | :--------------- | :--------------- | :------------------------ | :-------------------- |
| HugeGraph | 0.629            | 5.711            | 5.243                     | 67.033                |
| Titan     | 10.15            | 108.569          | 150.266                   | 1217.944              |
| Neo4j     | 3.884            | 18.938           | 24.890                    | 281.537               |
| Teamo     | 9.79             | 126.92           | 107.07                    | 未测试                |

##### 遍历性能

待测试

##### 其他性能

待测试



**（暂时不考虑性能问题，即使有做相应测试，但测试环境不同，所以性能对比不具有严格的参考价值。）**



## Related Efforts

- [Titan](https://github.com/thinkaurelius/titan) ([http://titandb.io](http://titandb.io/))
- [Cayley](https://github.com/cayleygraph/cayley)
- [Neo4j](https://github.com/neo4j/neo4j) ([http://neo4j.com](http://neo4j.com/))
- [HugeGraph](https://github.com/hugegraph/hugegraph)
- ...



## License

Teamo is licensed under [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).