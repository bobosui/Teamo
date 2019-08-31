#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
from teamo import Teamo

conn = sqlite3.connect(':memory:')
# 基于数据库的抽象图
graph = Teamo(conn, db='sqlite3')
# 从数据库中构建空图
graph.init()
# 创造一个遍历对象
g = graph.traversal()

print('Create Gremlin Modern Graph...')
# 插入六个点
v1 = g.addV().label('person').data('{"name":"marko","age":29}').id()
v2 = g.addV().label('person').data('{"name":"vadas","age":27}').id()
v3 = g.addV().label('software').data('{"name":"lop","lang":"java"}').id()
v4 = g.addV().label('person').data('{"name":"josh","age":32}').id()
v5 = g.addV().label('software').data('{"name":"ripple","lang":"java"}').id()
v6 = g.addV().label('person').data('{"name":"peter","age":35}').id()
# 插入六条边
g.addE(v1, v2).label('knows').data('{"weight:0.5"}')
g.addE(v1, v4).label('knows').data('{"weight:1.0"}')
g.addE(v1, v3).label('created').data('{"weight:0.4"}')
g.addE(v4, v5).label('created').data('{"weight:1.0"}')
g.addE(v4, v3).label('created').data('{"weight:0.4"}')
g.addE(v6, v3).label('created').data('{"weight:0.2"}')
print('Done!')

###################################################
# 以下是实现Gremlin Repeat的第一个样例 即
#   g.V(1).repeat(out()).times(2).path().by('name')
# 期望结果为
#   ==>[marko,josh,ripple]
#   ==>[marko,josh,lop]
paths = []
v_list = g.V(1).pack('p').identity()
paths = [ [v] for v in v_list ]
for i in range(2):
    new_paths = []
    for p in paths:
        v_list = g.V(p[-1]).out().pack('p').identity()
        if len(v_list) == 0:
            continue
        new_paths.extend([ p + [v] for v in v_list ])
    paths = new_paths
result = [ [ g.V(step).values('name')[0] for step in p ] for p in paths ]
print(result)
#
#   结束
###################################################

conn.close()
