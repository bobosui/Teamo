#!/usr/bin/python
# -*- coding: utf-8 -*-

from teamo import Teamo
import sqlite3
import pymysql
import json
import random
import time
import datetime

# 甚至不用异常 直接退出 (原处理为 包装异常 并且不处理异常 直接抛出 结束程序)
def shouldNotHappen(message: str) -> None:
    raise Exception(message)


def generate_gremlin_modern_graph(conn, db: str):
    print('构建Gremlin Modern Graph Runing...')
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    v1 = g.addV().label('person').data('{"name":"marko","age":29}').id()
    v2 = g.addV().label('person').data('{"name":"vadas","age":27}').id()
    v3 = g.addV().label('software').data('{"name":"lop","lang":"java"}').id()
    v4 = g.addV().label('person').data('{"name":"josh","age":32}').id()
    v5 = g.addV().label('software').data('{"name":"ripple","lang":"java"}').id()
    v6 = g.addV().label('person').data('{"name":"peter","age":35}').id()
    g.addE(v1, v2).label('knows').data('{"weight:0.5"}')
    g.addE(v1, v4).label('knows').data('{"weight:1.0"}')
    g.addE(v1, v3).label('created').data('{"weight:0.4"}')
    g.addE(v4, v5).label('created').data('{"weight:1.0"}')
    g.addE(v4, v3).label('created').data('{"weight:0.4"}')
    g.addE(v6, v3).label('created').data('{"weight:0.2"}')
    print('Done!')


def generate_gremlin_modern_graph_in_branch(conn, db: str):
    print('Build Gremlin Modern Graph Runing In Batch...')
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    g.addV().label('person').data('{"name":"marko","age":29}').id()
    g.addV().label('person').data('{"name":"vadas","age":27}').id()
    g.addV().label('software').data('{"name":"lop","lang":"java"}').id()
    g.addV().label('person').data('{"name":"josh","age":32}').id()
    g.addV().label('software').data('{"name":"ripple","lang":"java"}').id()
    g.addV().label('person').data('{"name":"peter","age":35}').id()
    graph._add_edge_in_branch([ (1, 2), (1, 4), (1, 3), (4, 5), (4, 3), (6, 3) ])
    # 找到点A所有出边 点B所有入边 取交集 即得 A -> B 的边
    def take_e(va: int, vb: int) -> int:
        va_out_e = g.V(va).outE().identity()
        vb_in_e = g.V(vb).inE().identity()
        e_set = set(va_out_e) & set(vb_in_e)
        if len(e_set) != 1:
            shouldNotHappen('应该只有一条由A到B的边（简单图）')
        return e_set.pop()
    # 为边插入标签属性
    g.E(take_e(1, 2)).label('knows').data('{"weight:0.5"}')
    g.E(take_e(1, 4)).label('knows').data('{"weight:1.0"}')
    g.E(take_e(1, 3)).label('created').data('{"weight:0.4"}')
    g.E(take_e(4, 5)).label('created').data('{"weight:1.0"}')
    g.E(take_e(4, 3)).label('created').data('{"weight:0.4"}')
    g.E(take_e(6, 3)).label('created').data('{"weight:0.2"}')
    print('Done!')


def generate_sparse_graph(conn, db: str, vertex_number: int, out_degree: int):
    if out_degree >= vertex_number:
        shouldNotHappen('出度必须小于点数')
    # 随机数设定种子 保证每次生成的图都是一样的
    random.seed('pyGraph')
    # MySQL使用MyIsam引擎
    # cur = conn.cursor()
    # if db.lower() == 'mysql':
    #     cur.execute("SET storage_engine=MYISAM;")
    # 基于数据库的抽象图
    graph = Teamo(conn, db=db)
    # 若是sqlite3，关闭智能commit模式，便于自己操作事务
    if graph.get_db_name().lower() == 'SQLite3'.lower():
        conn.isolation_level = None
    # 删除已有表 慎用
    graph.destroy()
    # 从数据库中构建空图
    graph.init()
    g = graph.traversal()
    cur = conn.cursor()
    # 单个事务，批量插入数据
    cur.execute("BEGIN")
    # 插点
    for _ in range(vertex_number):
        g.addVinRaw()
    # 随机插入边
    all_vertex = [ i + 1 for i in range(vertex_number) ]
    random.shuffle(all_vertex)
    count = 0
    for from_vertex in all_vertex:
        count = count + 1
        for to_vertex in random.sample(all_vertex, random.randint(0, 2 * out_degree)):
            g.addEinRaw(from_vertex, to_vertex)
        print('vertex {} done. {}%'.format(count, count * 100 / vertex_number))
    cur.execute("COMMIT")


def generate_email_enron_graph(conn, db: str):
    graph = Teamo(conn, db=db)
    # 若是sqlite3，关闭智能commit模式，便于自己操作事务
    if graph.get_db_name().lower() == 'SQLite3'.lower():
        conn.isolation_level = None
    graph.destroy()
    graph.init()
    g = graph.traversal()
    cur = conn.cursor()
    cur.execute("BEGIN")
    # Nodes: 36692 Edges: 367662
    for _ in range(36692):
        g.addVinRaw()
    with open('Email-Enron.txt') as f:
        count = 0
        # 解析一行中两个数字 并且增一（为了使点id由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原Email-Enron.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        for line in lines:
            g.addEinRaw(line[0], line[1])
            count = count + 1
            if count % 10000 == 0:
                print('edge {} done. {:8.2f}%'.format(count, count * 100 / 367662))
        print('edge {} done. {:8.2f}%'.format(count, count * 100 / 367662))
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
    cur.execute("COMMIT")


def generate_email_enron_graph_in_branch(conn, db: str):
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    # Nodes: 36692 Edges: 367662
    for _ in range(36692):
        g.addVinRaw()
    conn.commit()
    with open('Email-Enron.txt') as f:
        # 解析一行中两个数字 并且增一（为了使点id由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原Email-Enron.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        g._graph._add_edge_in_branch(lines)
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))


def generate_amazon0601_graph(conn, db: str):
    graph = Teamo(conn, db=db)
    # 若是sqlite3，关闭智能commit模式，便于自己操作事务
    if graph.get_db_name().lower() == 'SQLite3'.lower():
        conn.isolation_level = None
    graph.destroy()
    graph.init()
    g = graph.traversal()
    cur = conn.cursor()
    cur.execute("BEGIN")
    # Nodes: 403394 Edges: 3387388
    for _ in range(403394):
        g.addVinRaw()
    with open('Amazon0601.txt') as f:
        count = 0
        # 解析一行中两个数字 并且增一（为了使点id由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原Amazon0601.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        for line in lines:
            g.addEinRaw(line[0], line[1])
            count = count + 1
            if count % 100000 == 0:
                print('edge {} done. {:8.2f}%'.format(count, count * 100 / 3387388))
        print('edge {} done. {:8.2f}%'.format(count, count * 100 / 3387388))
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
    cur.execute("COMMIT")


def generate_amazon0601_graph_in_branch(conn, db: str):
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    # Nodes: 403394 Edges: 3387388
    for _ in range(403394):
        g.addVinRaw()
    conn.commit()
    with open('Amazon0601.txt') as f:
        # 解析一行中两个数字 并且增一（为了使点id由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原Amazon0601.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        g._graph._add_edge_in_branch(lines)
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))


def generate_com_youtube_ungraph_graph(conn, db: str):
    graph = Teamo(conn, db=db)
    # 若是sqlite3，关闭智能commit模式，便于自己操作事务
    if graph.get_db_name().lower() == 'SQLite3'.lower():
        conn.isolation_level = None
    graph.destroy()
    graph.init()
    g = graph.traversal()
    cur = conn.cursor()
    cur.execute("BEGIN")
    # Nodes: 1134890(有问题 暂定 1157827) Edges: 2987624
    for _ in range(1157827):
        g.addVinRaw()
    with open('com-youtube.ungraph.txt') as f:
        count = 0
        # 解析一行中两个数字 不需要增一（文件中id本身就是由1开始）
        to_int = lambda x: (int(x[0]), int(x[1]))
        # 4是硬编码 默认原com-youtube.ungraph.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        for line in lines:
            g.addEinRaw(line[0], line[1])
            count = count + 1
            if count % 100000 == 0:
                print('edge {} done. {:8.2f}%'.format(count, count * 100 / 2987624))
        print('edge {} done. {:8.2f}%'.format(count, count * 100 / 2987624))
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
    cur.execute("COMMIT")


def generate_com_youtube_ungraph_graph_in_branch(conn, db: str):
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    # Nodes: 1134890(有问题 暂定 1157827) Edges: 2987624
    for _ in range(1157827):
        g.addVinRaw()
    conn.commit()
    with open('com-youtube.ungraph.txt') as f:
        # 解析一行中两个数字 不需要增一（文件中id本身就是由1开始）
        to_int = lambda x: (int(x[0]), int(x[1]))
        # 4是硬编码 默认原com-youtube.ungraph.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        g._graph._add_edge_in_branch(lines)
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))


def generate_com_lj_ungraph_graph(conn, db: str):
    graph = Teamo(conn, db=db)
    # 若是sqlite3，关闭智能commit模式，便于自己操作事务
    if graph.get_db_name().lower() == 'SQLite3'.lower():
        conn.isolation_level = None
    graph.destroy()
    graph.init()
    g = graph.traversal()
    cur = conn.cursor()
    cur.execute("BEGIN")
    # Nodes: 3997962(有问题 暂定为 4040000) Edges: 34681189
    for _ in range(4040000):
        g.addVinRaw()
    with open('com-lj.ungraph.txt') as f:
        count = 0
        # 解析一行中两个数字 不需要增一（文件中id本身就是由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原com-youtube.ungraph.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        for line in lines:
            g.addEinRaw(line[0], line[1])
            count = count + 1
            if count % 1000000 == 0:
                print('edge {} done. {:8.2f}%'.format(count, count * 100 / 34681189))
        print('edge {} done. {:8.2f}%'.format(count, count * 100 / 34681189))
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
    cur.execute("COMMIT")


def generate_com_lj_ungraph_graph_in_branch(conn, db: str):
    graph = Teamo(conn, db=db)
    graph.destroy()
    graph.init()
    g = graph.traversal()
    # Nodes: 3997962(有问题 暂定为 4040000) Edges: 34681189
    for _ in range(4040000):
        g.addVinRaw()
    conn.commit()
    with open('com-lj.ungraph.txt') as f:
        # 解析一行中两个数字 不需要增一（文件中id本身就是由1开始）
        to_int = lambda x: (int(x[0]) + 1, int(x[1]) + 1)
        # 4是硬编码 默认原com-youtube.ungraph.txt仅有四行注释
        lines = [ to_int(line.split()) for line in f.readlines()[4:] ]
        t = time.time()
        g._graph._add_edge_in_branch(lines)
        elapsed = time.time() - t
        print('[Raw] Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))


def modify_test_on_greamlin_modern_graph(conn, db: str) -> None:
    graph = Teamo(conn, db=db)
    g = graph.traversal()
    # 使用addV, addE
    v1 = g.V(1).id()
    v4 = g.V(4).id()
    v6 = g.V(6).id()
    v7 = g.addV().label('person').data('{"name":"jeff","age":34}').id()
    g.addE(v7, v1).label('knows').data('{"weight:0.7"}')
    g.addE(v4, v7).label('knows').data('{"weight:0.1"}')
    g.addE(v7, v6).label('created').data('{"weight:0.5"}')
    # 使用drop
    g.V(6).drop()
    g.V(1).outE('created').drop()
    g.V(4).drop()
    g.V(2).drop()
    g.V().drop()


# 基于gremlin modern graph的只读测试
def query_test_on_gremlin_modern_graph(conn, db: str) -> None:
    print('基于gremlin modern graph的只读测试 Running...')

    graph = Teamo(conn, db=db)
    g = graph.traversal()

    #############################
    #       期待实现的功能       #
    #############################

    ########################################
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
    #       结束
    ########################################

    g.V().hasLabel('person').values('age')

    ########################################
    # 以下是Gremlin Choose Step的样例
    # 保证map时有序 一一对应
    person_age = g.V().hasLabel('person').pack('p').values('age')
    # 数据对应的id
    person = g.package('p')
    (yes, no) = ([], [])
    for i, age in enumerate(person_age):
        yes.append(person[i]) if age <= 30 else no.append(person[i])
    y_name = g.V(*yes).in_().values('name')
    n_name = g.V(*no).out().values('name')
    result = y_name + n_name
    print(result)
    #
    #     结束
    ########################################

    # 利用filter找出所有的人
    gt = g.V().hasLabel('person').hasLabel('software').identity()
    gt = g.E().hasLabel('created').outV().dedup().identity()

    w = None # 无实际作用 用于绑定返回值便于查看
    # 获取V(1)邻接点的data和label
    w = g.V(1).out().pack('s').data()
    s = g.package('s')
    w = g.V(*s).label()
    # 也可以写
    s = g.V(1).out().identity()
    w = g.V(*s).data()
    w = g.V(*s).label()
    print(w) # 使用以防提示unused 到时候补充GraphTraversal.string

    #############################
    #         查询类功能         #
    #############################

    # gt 和 t 只是为了打断点时看数值
    _gt = g.V()
    _gt = g.E()
    _gt = g.V(6)
    _gt = g.V(2, 3, 4)
    _gt = g.E(1)
    _gt = g.E(3, 6).identity()
    _t = g.V(1).label()
    _t = g.E(4).label()
    _t = g.V(3).data()
    _t = g.E(2).data()

    _gt = g.V(1).outE()
    _gt = g.V(1).outE('knows')
    _gt = g.V(1).outE('knows', 'created')
    _gt = g.V(1).outE('know')

    _gt = g.V(3).inE().outV().pack('hey')
    _gt = g.package('hey')
    _gt = g.V(3).inE('knows').outV().identity()
    _gt = g.V(3).inE('knows', 'created')
    _gt = g.V(4).inE('knows')
    _gt = g.V(1).inE()

    _gt = g.E(1).inV()
    _gt = g.E(2, 4).inV()
    _gt = g.E().inV()

    _gt = g.V(1).outE().inV()

    _gt = g.V(1).out('knows').identity()

    _gt = g.V(4).both()
    _gt = g.V(4).both('knows')
    _gt = g.V(4).both('created')
    _gt = g.V(1).both()
    _gt = g.V(1).both('created')
    _gt = g.V(1).both('creat')

    _gt = g.V(4).bothE()
    _gt = g.V(1).bothE('created')
    _gt = g.V(3).bothE()
    _gt = g.V(6).bothE('knows')

    _gt = g.E(3).bothV()
    _gt = g.E(1, 5, 6).bothV()

    _gt = g.V().pack('p1')
    _gt = g.unpackV('p1').identity()
    _gt = g.V(2, 4, 5).pack('p2')
    _gt = g.unpackV('p2').identity()

    print('Pass!')


def sqlite_mysql_read_write_test():
    # 连接数据库
    sqlite_conn = sqlite3.connect('storage/gremlin-modern-graph.sqlite')
    mysql_conn = pymysql.connect(host='localhost', port=3306,
        user='root', password='teamo',
        db='gremlin_modern_graph', charset='utf8', cursorclass=pymysql.cursors.Cursor)
    # 写测试
    generate_gremlin_modern_graph(sqlite_conn, 'sqlite3')
    generate_gremlin_modern_graph(mysql_conn, 'mysql')
    # 读测试
    query_test_on_gremlin_modern_graph(sqlite_conn, 'sqlite3')
    query_test_on_gremlin_modern_graph(mysql_conn, 'mysql')
    # 关闭数据库
    sqlite_conn.close()
    mysql_conn.close()


def query_test_on_big_sparse_graph(conn, db: str) -> None:
    print('基于 “一千万个点与平均出度为十（一亿条边）的稀疏图” 的只读测试 Running...')
    graph = Teamo(conn, db=db)
    g = graph.traversal()
    g.V()


# FN(Find Neighbor), 遍历所有vertex, 根据vertex查邻接edge, 通过edge和vertex查other vertex
def find_neighbor(conn, db: str):
    graph = Teamo(conn, db=db)
    g = graph.traversal()
    g.V().out()


# FA(Find Adjacent), 遍历所有edge，根据edge获得source vertex和target vertex
def find_adjacent(conn, db: str):
    graph = Teamo(conn, db=db)
    g = graph.traversal()
    g.E().bothV()


def test_mini():
    sqlite_conn = sqlite3.connect('gremlin_modern_graph.sqlite')
    generate_gremlin_modern_graph_in_branch(sqlite_conn, 'sqlite3')
    query_test_on_gremlin_modern_graph(sqlite_conn, 'sqlite3')
    modify_test_on_greamlin_modern_graph(sqlite_conn, 'sqlite3')
    sqlite_conn.close()


def main():
    # sqlite_mysql_read_write_test()
    # test_mini()
    # mysql_conn = pymysql.connect(host='localhost', port=3306,
    #     user='root', password='teamo',
    #     db='big_graph', charset='utf8', cursorclass=pymysql.cursors.Cursor)
    # generate_big_graph(mysql_conn, 'mysql', 1000)
    # mysql_conn.close()
    # sqlite_conn = sqlite3.connect('com-lj-ungraph.sqlite')
    # sqlite_conn = sqlite3.connect('amazon0601.sqlite')
    # sqlite_conn = sqlite3.connect('email-enron.sqlite')
    # sqlite_conn = sqlite3.connect('com-youtube-ungraph.sqlite')
    # sqlite_conn = sqlite3.connect('gremlin_modern_graph.sqlite')
    sqlite_conn = sqlite3.connect('storage/tmp.sqlite')
    # generate_com_lj_ungraph_graph_in_branch(sqlite_conn, 'sqlite3')
    # generate_email_enron_graph_in_branch(sqlite_conn, 'sqlite3')
    # generate_amazon0601_graph_in_branch(sqlite_conn, 'sqlite3')
    # generate_com_youtube_ungraph_graph_in_branch(sqlite_conn, 'sqlite3')
    # generate_gremlin_modern_graph_in_branch(sqlite_conn, 'sqlite3')
    # query_test_on_gremlin_modern_graph(sqlite_conn, 'sqlite3')
    # modify_test_on_greamlin_modern_graph(sqlite_conn, 'sqlite3')
    # generate_gremlin_modern_graph(sqlite_conn, 'sqlite3')
    # generate_email_enron_graph(sqlite_conn, 'sqlite3')
    # generate_big_graph(sqlite_conn, 'sqlite3', 2000)
    # generate_sparse_graph(sqlite_conn, 'sqlite3', 10000000, 10)
    # query_test_on_big_sparse_graph(sqlite_conn, 'sqlite3')
    # modify_test_on_greamlin_modern_graph(sqlite_conn, 'sqlite3')
    # generate_amazon0601_graph(sqlite_conn, 'sqlite3')
    # generate_com_youtube_ungraph_graph(sqlite_conn, 'sqlite3')
    # generate_com_lj_ungraph_graph(sqlite_conn, 'sqlite3')
    # find_neighbor(sqlite_conn, 'sqlite3')
    # find_adjacent(sqlite_conn, 'sqlite3')
    sqlite_conn.close()

if __name__ == "__main__":
    t = time.time()
    main()
    elapsed = time.time() - t
    print('Time waste: {} => {:.2f}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
