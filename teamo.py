#!/usr/bin/python
# -*- coding: utf-8 -*-

from typing import Optional, Union, Sequence, Tuple, Dict, Any, List
import sqlite3
import json
import pymysql
import random
import time
import datetime


# 甚至不用异常 直接退出 (原处理为 包装异常 并且不处理异常 直接抛出 结束程序)
def shouldNotHappen(message: str) -> None:
    raise Exception(message)

# 一个图 就是一个数据库 图只是抽象表现 内在结构是关系型数据库
# 这个图有图这种数据结构的常用API实现
# 仅考虑简单图（尽管似乎可以实现多重图）
class Teamo:
    
    # 构造函数做的事是创建一个空图
    def __init__(self, conn, *, db: str):
        # 判断参数合法性
        db_api_2_methods = [ 'close', 'commit', 'rollback', 'cursor' ]
        for method in db_api_2_methods:
            if not callable(getattr(conn, method)):
                shouldNotHappen('不符合Python DB-API 2.0协议')
        self._conn = conn # 此图所在的数据库的连接
        self._base_db = None
        if db.lower() == 'SQLite3'.lower():
            self._base_db = 'SQLite3'
            self._ph = '?'
        elif db.lower() == 'MySQL'.lower():
            self._base_db = 'MySQL'
            self._ph = '%s'
        else:
            shouldNotHappen('不支持此数据库')

    def get_db_name(self):
        return str(self._base_db)

    def get_connection(self):
        return self._conn
    
    # ----------------- 以下是图常用API ----------------- #

    # 创建图
    def init(self):
        c = self._conn.cursor()
        c.execute('''
            CREATE TABLE `Edge` (
                `id`	INTEGER PRIMARY KEY NOT NULL UNIQUE /*!40101 AUTO_INCREMENT */,
                `tail`	INTEGER NOT NULL,
                `head`	INTEGER NOT NULL,
                `backward`	INTEGER,
                `forward`	INTEGER,
                `revback`	INTEGER,
                `revfor`	INTEGER,
                `label`   TEXT,
                `data`	TEXT
            );
        ''')
        c.execute('''
            CREATE TABLE `Vertex` (
                `id`	INTEGER PRIMARY KEY NOT NULL UNIQUE /*!40101 AUTO_INCREMENT */,
                `in_edge`	INTEGER,
                `out_edge`	INTEGER,
                `label`   TEXT,
                `data`	TEXT
            );
        ''')
        self._conn.commit()

    # [!] Warning: 删表跑路 后果很严重
    def destroy(self):
        c = self._conn.cursor()
        c.execute("DROP TABLE IF EXISTS `Vertex`;")
        c.execute("DROP TABLE IF EXISTS `Edge`;")
        self._conn.commit()

    ##########################################
    #       add/remove the vertex/edge       #
    ##########################################

    # 添加顶点x，如果它不存在 （谨慎使用do_commit参数 后果自负）
    def _add_vertex(self, *, do_commit: bool = True) -> int:
        c = self._conn.cursor()
        c.execute(
            '''INSERT INTO Vertex (`in_edge`, `out_edge`, `label`, `data`) VALUES (NULL, NULL, NULL, NULL)''')
        # 获取新插入的顶点的id
        new_vertex_id = c.lastrowid
        if do_commit:
            self._conn.commit()
        return new_vertex_id

    # 删除顶点x，如果它在那里 （谨慎使用do_commit参数 后果自负）
    def _remove_vertex(self, vertex_id: int, *, do_commit: bool = True) -> None:
        c = self._conn.cursor()
        # 先获得顶点的出边链表引用和入边链表引用（根据惯例，先处理出后处理入）
        c.execute(
            '''SELECT `in_edge`, `out_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        (in_edge, out_edge) = c.fetchone()
        current_id = out_edge
        while current_id is not None:
            # 首次进入循环 获得出边链表头节点
            c.execute(
                '''SELECT `tail`, `head`, `backward`, `forward`, `revback`, `revfor` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph),
                (current_id,)
            )
            (tail, head, backward, forward, revback, revfor) = c.fetchone()
            # 衔接指向此边节点的和此边节点所指向的包含此节点的某个入边链表（即在双向链表中移除此节点）
            if revback is None:
                # 它一定是作为某入边链表的头 因为一条边必然会同时作为出边和入边
                c.execute(
                    '''UPDATE Vertex SET `in_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (backward, head)
                )
                # 记得补上双向链表中的逆向指针为None
                if backward is not None:
                    c.execute(
                        '''UPDATE Edge SET `revback`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (None, backward)
                    )
            else:
                # 更新此节点所在的入边链表中上一个边节点
                c.execute(
                    '''UPDATE Edge SET `backward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (backward, revback)
                )
                if backward is not None:
                    # 更新此节点所在的入边链表中下一个边节点的逆向指针
                    c.execute(
                        '''UPDATE Edge SET `revback`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (revback, backward)
                    )
            c.execute(
                '''DELETE FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            current_id = forward
        current_id = in_edge
        while current_id is not None:
            # 首次进入循环 获得入边链表头节点
            c.execute(
                '''SELECT `tail`, `head`, `backward`, `forward`, `revback`, `revfor` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            (tail, head, backward, forward, revback, revfor) = c.fetchone()
            # 衔接指向此边节点的和此边节点所指向的包含此节点的某个出边链表（即在双向链表中移除此节点）
            if revfor is None:
                # 它一定是作为某出边链表的头 因为一条边必然会同时作为出边和入边
                c.execute(
                    '''UPDATE Vertex SET `out_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (forward, tail)
                )
                # 记得补上双向链表中的逆向指针为None
                if forward is not None:
                    c.execute(
                        '''UPDATE Edge SET `revfor`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (None, forward)
                    )
            else:
                # 更新此节点所在的出边链表中上一个边节点
                c.execute(
                    '''UPDATE Edge SET `forward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (forward, revfor)
                )
                if forward is not None:
                    # 更新此节点所在的出边链表中下一个边节点的逆向指针
                    c.execute(
                        '''UPDATE Edge SET `revfor`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (revfor, forward)
                    )
            c.execute(
                '''DELETE FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            current_id = backward
        c.execute(
            '''DELETE FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        if do_commit:
            self._conn.commit()

    # 添加x到y的一条边，返回这条边 （谨慎使用do_commit参数 后果自负）
    def _add_edge(self, from_vertex: int, to_vertex: int, *, do_commit: bool = True) -> int:
        c = self._conn.cursor()
        # 插入一条新边到 Edge Table
        c.execute(
            '''INSERT INTO Edge (`tail`, `head`) VALUES ({ph}, {ph})'''.format(ph=self._ph), 
            (from_vertex, to_vertex)
        )
        # 获取新插入的边的id
        new_edge_id = c.lastrowid
        # ----------- 这里是关于放置新边在尾顶点的出边out_edge链表头部的代码 ----------- #
        # 获取from_vertex也就是tail的出边链表表头引用
        c.execute(
            '''SELECT `out_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (from_vertex,)
        )
        (out_id,) = c.fetchone()
        if out_id is None:
            # 空引用则直接连到新的边上，因为新边在此作为链表表头，所以不需要反向引用，revfor为空
            c.execute(
                '''UPDATE Vertex SET `out_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, from_vertex)
            )
        else:
            # 设置新边的下一个节点为尾顶点的出边out_edge链表的头节点，也就是顶替此链表头节点的位置
            c.execute(
                '''UPDATE Edge SET `forward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (out_id, new_edge_id)
            )
            # 将旧的链表头节点的逆向参数revfor补上
            c.execute(
                '''UPDATE Edge SET `revfor`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, out_id)
            )
            # 将尾顶点的出边设置为新的头节点（也就是新边）的引用
            c.execute(
                '''UPDATE Vertex SET `out_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, from_vertex)
            )
            # 至此出边链表的更新完成
        # ----------- 这里是关于放置新边在头顶点的出边in_edge链表头部的代码 ----------- #
        # 获取to_vertex也就是head的出边链表表头引用
        c.execute(
            '''SELECT `in_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (to_vertex,)
        )
        (in_id,) = c.fetchone()
        if in_id is None:
            # 空引用则直接连到新的边上，因为新边在此作为链表表头，所以不需要反向引用，revback为空
            c.execute(
                '''UPDATE Vertex SET `in_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, to_vertex)
            )
        else:
            # 设置新边的下一个节点为头顶点的入边in_edge链表的头节点，也就是顶替此链表头节点的位置
            c.execute(
                '''UPDATE Edge SET `backward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (in_id, new_edge_id)
            )
            # 将旧的链表头节点的逆向参数revback补上
            c.execute(
                '''UPDATE Edge SET `revback`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, in_id)
            )
            # 将头顶点的入边设置为新的头节点（也就是新边）的引用
            c.execute(
                '''UPDATE Vertex SET `in_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (new_edge_id, to_vertex)
            )
            # 至此入边链表的更新完成
        if do_commit:
            self._conn.commit()
        return new_edge_id

    # 删除边 （谨慎使用do_commit参数 后果自负）
    def _remove_edge(self, edge_id: int, *, do_commit: bool = True) -> None:
        c = self._conn.cursor()
        c.execute(
            '''SELECT `tail`, `head`, `backward`, `forward`, `revback`, `revfor` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph),
            (edge_id,)
        )
        (tail, head, backward, forward, revback, revfor) = c.fetchone()
        # 在出边链表中删除节点
        if revfor is None:
            # 此边节点作为出边链表头结点
            c.execute(
                '''UPDATE Vertex SET `out_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (forward, tail)
            )
            if forward is not None:
                # 记得处理逆向指针
                c.execute(
                    '''UPDATE Edge SET `revfor`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (None, forward)
                )
        else:
            # 此边节点在出边链表非头部位置
            c.execute(
                '''UPDATE Edge SET `forward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (forward, revfor)
            )
            if forward is not None:
                c.execute(
                    '''UPDATE Edge SET `revfor`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (revfor, forward)
                )
        # 在入边链表中删除节点
        if revback is None:
            # 此边节点作为入边链表头结点
            c.execute(
                '''UPDATE Vertex SET `in_edge`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (backward, head)
            )
            if backward is not None:
                # 记得处理逆向指针
                c.execute(
                    '''UPDATE Edge SET `revback`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (None, backward)
                )
        else:
            # 此边节点在入边链表非头部位置
            c.execute(
                '''UPDATE Edge SET `backward`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (backward, revback)
            )
            if backward is not None:
                c.execute(
                    '''UPDATE Edge SET `revback`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (revback, backward)
                )
        c.execute(
            '''DELETE FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (edge_id,)
        )
        if do_commit:
            self._conn.commit()

    def _get_all_vertex(self) -> Sequence[int]:
        c = self._conn.cursor()
        # 获取Vertex Table中所有的rows中的id
        c.execute(
            '''SELECT `id` FROM Vertex'''
        )
        vertex_list = [ v[0] for v in c.fetchall() ]
        return vertex_list

    def _get_all_edge(self) -> Sequence[int]:
        c = self._conn.cursor()
        # 获取Edge Table中所有的rows中的id
        c.execute(
            '''SELECT `id` FROM Edge'''
        )
        edge_list = [ e[0] for e in c.fetchall() ]
        return edge_list

    def _get_some_vertex(self, ids: Sequence[int]) -> Sequence[int]:
        # 通过id获得点 然而在这个项目中 点的id就代表点 只需要检查id是否合法
        if len(ids) == 0:
            return ids
        c = self._conn.cursor()
        c.execute(
            '''SELECT 1 FROM Vertex WHERE `id` IN ({ph}'''.format(ph=self._ph) + 
            ',{ph}'.format(ph=self._ph) * (len(ids) - 1) + ')'
            , ids
        )
        if len(c.fetchall()) != len(ids):
            shouldNotHappen('存在不合法ID')
        return ids
        
    def _get_some_edge(self, ids: Sequence[int]) -> Sequence[Tuple[int, str]]:
        # 通过id获得边 然而在这个项目中 边的id就代表边 只需要检查id是否合法
        if len(ids) == 0:
            return ids
        c = self._conn.cursor()
        c.execute(
            '''SELECT 1 FROM Edge WHERE `id` IN ({ph}'''.format(ph=self._ph) + 
            ',{ph}'.format(ph=self._ph) * (len(ids) - 1) + ')'
            , ids
        )
        if len(c.fetchall()) != len(ids):
            shouldNotHappen('存在不合法ID')
        return ids

    ##########################################
    #         get/set the label/data         #
    ##########################################

    # 获取点上附着的数据
    def _get_vertex_data(self, vertex_id: int) -> Optional[str]:
        c = self._conn.cursor()
        c.execute('''SELECT `data` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,))
        (data,) = c.fetchone()
        return data

    # 设置点上附着的数据
    def _set_vertex_data(self, vertex_id: int, data: str):
        c = self._conn.cursor()
        c.execute('''UPDATE Vertex SET `data`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (data, vertex_id))
        self._conn.commit()

    # 获取点上附着的label
    def _get_vertex_label(self, vertex_id: int) -> Optional[str]:
        c = self._conn.cursor()
        c.execute('''SELECT `label` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,))
        (label,) = c.fetchone()
        return label

    # 设置点上附着的label
    def _set_vertex_label(self, vertex_id: int, label: str):
        c = self._conn.cursor()
        c.execute('''UPDATE Vertex SET `label`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (label, vertex_id))
        self._conn.commit()

    # 获取边上附着的数据
    def _get_edge_data(self, edge_id: int) -> Optional[str]:
        c = self._conn.cursor()
        c.execute('''SELECT `data` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (edge_id,))
        (data,) = c.fetchone()
        return data

    # 设置边上附着的数据 
    def _set_edge_data(self, edge_id: int, data: str):
        c = self._conn.cursor()
        c.execute('''UPDATE Edge SET `data`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (data, edge_id))
        self._conn.commit()

    # 获取边上附着的label
    def _get_edge_label(self, edge_id: int) -> Optional[str]:
        c = self._conn.cursor()
        c.execute('''SELECT `label` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (edge_id,))
        (label,) = c.fetchone()
        return label

    # 设置边上附着的label 
    def _set_edge_label(self, edge_id: int, label: str):
        c = self._conn.cursor()
        c.execute('''UPDATE Edge SET `label`={ph} WHERE `id`={ph}'''.format(ph=self._ph), (label, edge_id))
        self._conn.commit()

    # 获取点的所有出边
    def _get_out_edge(self, vertex_id: int) -> Sequence[int]:
        c = self._conn.cursor()
        edge_list = []
        # 先获得顶点的出边链表引用
        c.execute(
            '''SELECT `out_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        (out_edge,) = c.fetchone()
        current_id = out_edge
        while current_id is not None:
            # 获得出边链表头节点
            c.execute(
                '''SELECT `id`, `forward` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            (id_, forward) = c.fetchone()
            edge_list.append(id_)
            current_id = forward
        return edge_list

    def _get_out_edge_by_label(self, vertex_id: int, labels: Sequence[str]) -> Sequence[int]:
        c = self._conn.cursor()
        edge_list = []
        c.execute(
            '''SELECT `out_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        (out_edge,) = c.fetchone()
        current_id = out_edge
        while current_id is not None:
            # 获得出边链表头节点
            c.execute(
                '''SELECT `id`, `forward`, `label` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            (id_, forward, label) = c.fetchone()
            if label in labels:
                edge_list.append(id_)
            current_id = forward
        return edge_list

    # 获取点的所有入边
    def _get_in_edge(self, vertex_id: int) -> Sequence[int]:
        c = self._conn.cursor()
        edge_list = []
        # 先获得顶点的出边链表引用
        c.execute(
            '''SELECT `in_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        (in_edge,) = c.fetchone()
        current_id = in_edge
        while current_id is not None:
            # 获得出边链表头节点
            c.execute(
                '''SELECT `id`, `backward` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            (id_, backward) = c.fetchone()
            edge_list.append(id_)
            current_id = backward
        return edge_list

    def _get_in_edge_by_label(self, vertex_id: int, labels: Sequence[str]) -> Sequence[int]:
        c = self._conn.cursor()
        edge_list = []
        c.execute(
            '''SELECT `in_edge` FROM Vertex WHERE `id`={ph}'''.format(ph=self._ph), (vertex_id,)
        )
        (in_edge,) = c.fetchone()
        current_id = in_edge
        while current_id is not None:
            # 获得出边链表头节点
            c.execute(
                '''SELECT `id`, `backward`, `label` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (current_id,)
            )
            (id_, backward, label) = c.fetchone()
            if label in labels:
                edge_list.append(id_)
            current_id = backward
        return edge_list

    def _get_in_vertex(self, edge_id: int) -> int:
        c = self._conn.cursor()
        c.execute(
            '''SELECT `head` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (edge_id,)
        )
        (head,) = c.fetchone()
        return head

    def _get_out_vertex(self, edge_id: int) -> int:
        c = self._conn.cursor()
        c.execute(
            '''SELECT `tail` FROM Edge WHERE `id`={ph}'''.format(ph=self._ph), (edge_id,)
        )
        (tail,) = c.fetchone()
        return tail

    def _filter_vertex_by_labels(self, vertex_ids: Sequence[int], labels: Sequence[str]) -> Sequence[int]:
        if len(vertex_ids) == 0 or len(labels) == 0:
            return vertex_ids
        c = self._conn.cursor()
        args = list(vertex_ids) + list(labels)
        c.execute(
            '''SELECT `id` FROM Vertex WHERE `id` IN ({ph}'''.format(ph=self._ph) +
            ',{ph}'.format(ph=self._ph) * (len(vertex_ids) - 1) +
            ''') AND `label` IN ({ph}'''.format(ph=self._ph) +
            ',{ph}'.format(ph=self._ph) * (len(labels) - 1) +
            ')',
            args
        )
        result_vertexs = [ row[0] for row in c.fetchall() ]
        return result_vertexs

    def _filter_edge_by_labels(self, edge_ids: Sequence[int], labels: Sequence[str]) -> Sequence[int]:
        if len(edge_ids) == 0 or len(labels) == 0:
            return edge_ids
        c = self._conn.cursor()
        args = list(edge_ids) + list(labels)
        c.execute(
            '''SELECT `id` FROM Edge WHERE `id` IN ({ph}'''.format(ph=self._ph) +
            ',{ph}'.format(ph=self._ph) * (len(edge_ids) - 1) +
            ''') AND `label` IN ({ph}'''.format(ph=self._ph) +
            ',{ph}'.format(ph=self._ph) * (len(labels) - 1) +
            ')',
            args
        )
        result_edges = [ row[0] for row in c.fetchall() ]
        return result_edges

    # TODO
    def traversal(self) -> 'GraphTraversal':
        return GraphTraversalSource(self)


# 整个GraphTraversal（图遍历）的起点
# 加边加点必须在起点处进行
class GraphTraversalSource:
    def __init__(self, graph: Teamo):
        self._graph = graph
        self._package_pool = {}

    ##########################################
    #          boot graph traversal          #
    ##########################################

    # return new traversal with some vertexs
    def V(self, *ids: int) -> 'GraphTraversal':
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        vertex_id_list = []
        # 我可以写成三元运算符但偏不
        if ids:
            vertex_id_list = self._graph._get_some_vertex(ids)
        else:
            vertex_id_list = self._graph._get_all_vertex()
        graph_traversal._set_vertex(vertex_id_list)
        return graph_traversal

    # return new traversal with some edges
    def E(self, *ids: int) -> 'GraphTraversal':
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        edge_id_list = []
        # 我可以写成三元运算符但偏不
        if ids:
            edge_id_list = self._graph._get_some_edge(ids)
        else:
            edge_id_list = self._graph._get_all_edge()
        graph_traversal._set_edge(edge_id_list)
        return graph_traversal

    # Add a new vertex to graph
    # Return new traversal with the new vertex
    def addV(self) -> 'GraphTraversal':
        vertex_id = self._graph._add_vertex()
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        graph_traversal._set_vertex([vertex_id])
        return graph_traversal

    # Add a new edge to graph
    # Return new traversal with the new edges
    def addE(self, from_vertex: int, to_vertex: int) -> 'GraphTraversal':
        edge_id = self._graph._add_edge(from_vertex, to_vertex)
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        graph_traversal._set_edge([edge_id])
        return graph_traversal

    # [!] Warning: You had better know what you are doing.
    def addVinRaw(self) -> 'GraphTraversal':
        vertex_id = self._graph._add_vertex(do_commit=False)
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        graph_traversal._set_vertex([vertex_id])
        return graph_traversal

    # [!] Warning: You had better know what you are doing.
    def addEinRaw(self, from_vertex: int, to_vertex: int) -> 'GraphTraversal':
        edge_id = self._graph._add_edge(from_vertex, to_vertex, do_commit=False)
        graph_traversal = GraphTraversal(self._graph, self._package_pool)
        graph_traversal._set_edge([edge_id])
        return graph_traversal

    # g.unpackV('key') == g.V(*g.package('key'))
    def unpackV(self, package_name: str) -> 'GraphTraversal':
        return self.V(*self.package(package_name))

    # g.unpackE('key') == g.E(*g.package('key'))
    def unpackE(self, package_name: str) -> 'GraphTraversal':
        return self.E(*self.package(package_name))

    # empty string == no args
    def package(self, package_name: Optional[str] = None) -> Any:
        if package_name is None:
            package_name = ''
        if package_name not in self._package_pool:
            shouldNotHappen('此货物不存在')
        return self._package_pool[package_name]

     
class GraphTraversal:
    def __init__(self, graph: Teamo, package_pool: Dict[str, Any]):
        self._graph = graph
        self._package_pool = package_pool
        self._vertexs = None # Optional[Sequence[int]]
        self._edges = None # Optional[Sequence[int]]

    def _is_vertex_in_use(self):
        return self._vertexs is not None

    def _is_edge_in_use(self):
        return self._edges is not None

    def _is_both_in_use(self):
        return self._is_vertex_in_use() and self._is_edge_in_use()
        
    def _is_none_in_use(self):
        return not (self._is_vertex_in_use() or self._is_edge_in_use())

    def _expect_vertex_in_use(self):
        if not self._is_vertex_in_use():
            shouldNotHappen('应该点集在用才对')
        
    def _expect_edge_in_use(self):
        if not self._is_edge_in_use():
            shouldNotHappen('应该边集在用才对')

    def _expect_both_in_use(self):
        if not self._is_both_in_use():
            shouldNotHappen('应该点集与边集同时在用才对')

    def _expect_none_in_use(self):
        if not self._is_none_in_use():
            shouldNotHappen('应该两个集都没在用才对')

    def _expect_vertex_or_edge_in_use(self):
        if self._is_none_in_use() or self._is_both_in_use():
            shouldNotHappen('应该点集或边集有且仅有其中一个在用才对')

    # @Deprecated
    def _push_vertex(self, vertex: int):
        self._vertexs.append(vertex)

    # @Deprecated
    def _push_edge(self, edge: int):
        self._edges.append(edge)

    def _set_vertex(self, vertexs: Sequence[int]):
        del self._vertexs
        # 浅拷贝
        self._vertexs = list(vertexs)

    def _set_edge(self, edges: Sequence[int]):
        del self._edges
        # 浅拷贝
        self._edges = list(edges)

    def _extend_vertex(self, vertexs: Sequence[int]):
        self._vertexs.extend(vertexs)

    def _extend_edge(self, edges: Sequence[int]):
        self._edges.extend(edges)

    def _clean_vertex(self):
        del self._vertexs
        self._vertexs = None

    def _clean_edge(self):
        del self._edges
        self._edges = None

    ##########################################
    #         gremlin vertex step            #
    ##########################################

    # Move to the outgoing adjacent vertices given the edge labels.
    def out(self, *labels: str) -> 'GraphTraversal':
        return self.outE(*labels).inV()
    
    # Move to the incoming adjacent vertices given the edge labels.
    def in_(self, *labels: str) -> 'GraphTraversal':
        return self.inE(*labels).outV()

    # Move to both the incoming and outgoing adjacent vertices given the edge labels.
    def both(self, *labels: str) -> 'GraphTraversal':
        self._expect_vertex_in_use()
        # 存放结果的点集
        vertex_list = []
        # 先找出边的入点
        out_edge_list = []
        # 下面与outE一致 不要问我为什么写重复的代码
        if labels:
            for v in self._vertexs:
                for e in self._graph._get_out_edge_by_label(v, labels):
                    out_edge_list.append(e)
        else:
            for v in self._vertexs:
                for e in self._graph._get_out_edge(v):
                    out_edge_list.append(e)
        # 下面与inV一致 同上
        for e in out_edge_list:
            v = self._graph._get_in_vertex(e)
            vertex_list.append(v)
        # 再找入边的出点
        in_edge_list = []
        # 下面与inE一致 同上
        if labels:
            for v in self._vertexs:
                for e in self._graph._get_in_edge_by_label(v, labels):
                    in_edge_list.append(e)
        else:
            for v in self._vertexs:
                for e in self._graph._get_in_edge(v):
                    in_edge_list.append(e)
        # 下面与inV一致 同上
        for e in in_edge_list:
            v = self._graph._get_out_vertex(e)
            vertex_list.append(v)
        # 最后结果存进去
        self._set_vertex(vertex_list)
        self._clean_edge()
        return self

    # Move to the outgoing incident edges given the edge labels.
    def outE(self, *labels: str) -> 'GraphTraversal':
        self._expect_vertex_in_use()
        edge_list = []
        if labels:
            # 注释如下
            for v in self._vertexs:
                for e in self._graph._get_out_edge_by_label(v, labels):
                    edge_list.append(e)
        else:
            # 对点集中每个点找出边
            for v in self._vertexs:
                # 对找到的出边边集进行生成Edge对象的列表
                for e in self._graph._get_out_edge(v):
                    edge_list.append(e)
        # 把Edge对象的列表保存起来到self这个Traversal里
        self._set_edge(edge_list)
        self._clean_vertex()
        return self
    
    # Move to the incoming incident edges given the edge labels.
    def inE(self, *labels: str) -> 'GraphTraversal':
        self._expect_vertex_in_use()
        edge_list = []
        if labels:
            # 注释如下
            for v in self._vertexs:
                for e in self._graph._get_in_edge_by_label(v, labels):
                    edge_list.append(e)
        else:
            # 对点集中每个点找入边
            for v in self._vertexs:
                # 对找到的入边边集进行生成Edge对象的列表
                for e in self._graph._get_in_edge(v):
                    edge_list.append(e)
        # 把Edge对象的列表保存起来到self这个Traversal里
        self._set_edge(edge_list)
        self._clean_vertex()
        return self

    # Move to both the incoming and outgoing incident edges given the edge labels.
    def bothE(self, *labels: str) -> 'GraphTraversal':
        self._expect_vertex_in_use()
        edge_list = []
        if labels:
            for v in self._vertexs:
                # 获取入边
                for e in self._graph._get_in_edge_by_label(v, labels):
                    edge_list.append(e)
                # 获取出边
                for e in self._graph._get_out_edge_by_label(v, labels):
                    edge_list.append(e)
        else:
            for v in self._vertexs:
                # 获取入边
                for e in self._graph._get_in_edge(v):
                    edge_list.append(e)
                # 获取出边
                for e in self._graph._get_out_edge(v):
                    edge_list.append(e)
        # 把Edge对象的列表保存起来到self这个Traversal里
        self._set_edge(edge_list)
        self._clean_vertex()
        return self

    # Move to the outgoing vertex.
    def outV(self) -> 'GraphTraversal':
        self._expect_edge_in_use()
        vertex_list = []
        for e in self._edges:
            v = self._graph._get_out_vertex(e)
            vertex_list.append(v)
        self._set_vertex(vertex_list)
        self._clean_edge()
        return self

    # Move to the incoming vertex.
    def inV(self) -> 'GraphTraversal':
        self._expect_edge_in_use()
        vertex_list = []
        for e in self._edges:
            v = self._graph._get_in_vertex(e)
            vertex_list.append(v)
        self._set_vertex(vertex_list)
        self._clean_edge()
        return self

    # Move to both vertices.
    def bothV(self) -> 'GraphTraversal':
        self._expect_edge_in_use()
        vertex_list = []
        for e in self._edges:
            in_v = self._graph._get_in_vertex(e)
            vertex_list.append(in_v)
            out_v = self._graph._get_out_vertex(e)
            vertex_list.append(out_v)
        self._set_vertex(vertex_list)
        self._clean_edge()
        return self

    ##########################################
    #         gremlin filter step            #
    ##########################################
    
    def hasLabel(self, *labels: str) -> 'GraphTraversal':
        self._expect_vertex_or_edge_in_use()
        if not labels:
            shouldNotHappen('需要参数 一个或多个labels')
        if self._is_vertex_in_use():
            result_vertexs = self._graph._filter_vertex_by_labels(self._vertexs, labels)
            self._set_vertex(result_vertexs)
        if self._is_edge_in_use():
            result_edges = self._graph._filter_edge_by_labels(self._edges, labels)
            self._set_edge(result_edges)
        return self

    ##########################################
    #      set/get label/data step (end)     #
    ##########################################

    # **但凡修改内容而非查询内容，都直接结束遍历(GraphTraversal)**
    # 另：获取或设置label和data 都直接结束遍历(GraphTraversal)

    # If label is not None, set the label of vertex(s)/edge(s)
    def label(self, label: Optional[str] = None) -> Union[Sequence[str], 'GraphTraversal']:
        self._expect_vertex_or_edge_in_use()
        if self._is_vertex_in_use():
            if label is None: # means 'get label'
                labels = []
                for v in self._vertexs:
                    labels.append(self._graph._get_vertex_label(v))
                # 注意这里有一个出口
                self._clean_vertex()
                return labels
            else:
                for v in self._vertexs:
                    self._graph._set_vertex_label(v, label)
        elif self._is_edge_in_use():
            if label is None: # means 'get label'
                labels = []
                for e in self._edges:
                    labels.append(self._graph._get_edge_label(e))
                # 注意这里有一个出口
                self._clean_edge()
                return labels
            else:
                for e in self._edges:
                    self._graph._set_edge_label(e, label)
        return self

    # If data is not None, set the data of vertex(s)/edge(s)
    def data(self, data: Optional[str] = None) -> Union[Sequence[str], 'GraphTraversal']:
        self._expect_vertex_or_edge_in_use()
        if self._is_vertex_in_use():
            if data is None: # means 'get data'
                datas = []
                for v in self._vertexs:
                    datas.append(self._graph._get_vertex_data(v))
                # 注意这里有一个出口
                self._clean_vertex()
                return datas
            else:
                for v in self._vertexs:
                    self._graph._set_vertex_data(v, data)
        elif self._is_edge_in_use():
            if data is None: # means 'get data'
                datas = []
                for e in self._edges:
                    datas.append(self._graph._get_edge_data(e))
                # 注意这里有一个出口
                self._clean_edge()
                return datas
            else:
                for e in self._edges:
                    self._graph._set_edge_data(e, data)
        return self

    ##########################################
    #              unknown step              #
    ##########################################

    # 返回traversal的边/点集的所有id
    def identity(self) -> Sequence[int]:
        self._expect_vertex_or_edge_in_use()
        ids = []
        if self._is_vertex_in_use():
            ids = self._vertexs
        elif self._is_edge_in_use():
            ids = self._edges
        self._clean_edge()
        self._clean_vertex()
        return ids

    # 返回当前唯一的点或边的id
    # 不用于identity()，id()只返回一个，当且仅当traversal中只有一个元素时合法
    def id(self) -> Sequence[int]:
        ids = self.identity()
        if len(ids) != 1:
            shouldNotHappen('id()当且仅当traversal中只有一个元素时调用合法')
        return ids[0]

    # 将当前的Traversal中的边集或点集保存到名称为var的package中，之后可以取出使用
    def pack(self, var: str) -> 'GraphTraversal':
        self._expect_vertex_or_edge_in_use()
        ids = []
        if self._is_vertex_in_use():
            ids = self._vertexs
        elif self._is_edge_in_use():
            ids = self._edges
        # 浅拷贝
        self._package_pool[var] = list(ids)
        return self

    # 将当前的Traversal中的边集或点集去重
    def dedup(self) -> 'GraphTraversal':
        self._expect_vertex_or_edge_in_use()
        if self._is_vertex_in_use():
            self._set_vertex(list(set(self._vertexs)))
        if self._is_edge_in_use():
            self._set_edge(list(set(self._edges)))
        return self

    # 即property。获取data里键值对中的值
    # 前提是以JSON为格式的data必须正确 并且有这个key
    def values(self, key: str) -> Any:
        self._expect_vertex_or_edge_in_use()
        if self._is_vertex_in_use():
            v_data = self.data()
            return [ json.loads(d)[key] for d in v_data ]
        if self._is_edge_in_use():
            e_data = self.data()
            return [ json.loads(d)[key] for d in e_data ]

    def drop(self) -> Any:
        self._expect_vertex_or_edge_in_use()
        if self._is_vertex_in_use():
            if len(self._vertexs) == 0:
                shouldNotHappen('不能drop一个只含有空的顶点集合的Traversal')
            for v in self._vertexs:
                self._graph._remove_vertex(v)
            self._clean_vertex()
        if self._is_edge_in_use():
            if len(self._edges) == 0:
                shouldNotHappen('不能drop一个只含有空的边集合的Traversal')
            for e in self._edges:
                self._graph._remove_edge(e)
            self._clean_edge()
            

def generate_gremlin_modern_graph(conn, db: str):
    print('构建Gremlin Modern Graph Runing...')
    # 基于数据库的抽象图
    graph = Teamo(conn, db=db)
    # 删除已有表 慎用
    graph.destroy()
    # 从数据库中构建空图
    graph.init()
    # 创造一个遍历对象
    g = graph.traversal()
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

def generate_big_graph(conn, db: str, vertex_number: int):
    # 随机数设定种子 保证每次生成的图都一样
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
    c = conn.cursor()
    # 优化，一个事务批量处理
    c.execute("BEGIN")
    # 插点
    for _ in range(vertex_number):
        g.addVinRaw()
    coin = 0.5
    for i in range(vertex_number):
        for j in range(vertex_number):
            if i == j:
                continue
            if random.random() < coin:
                g.addEinRaw(i + 1, j + 1)
        print('vertex {} done. {}%'.format(i+1, (i+1) * 100 / vertex_number))
    c.execute("COMMIT")

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
    gt = g.V()
    gt = g.E()
    gt = g.V(6)
    gt = g.V(2, 3, 4)
    gt = g.E(1)
    gt = g.E(3, 6).identity()
    t = g.V(1).label()
    t = g.E(4).label()
    t = g.V(3).data()
    t = g.E(2).data()

    gt = g.V(1).outE()
    gt = g.V(1).outE('knows')
    gt = g.V(1).outE('knows', 'created')
    gt = g.V(1).outE('know')

    gt = g.V(3).inE().outV().pack('hey')
    gt = g.package('hey')
    gt = g.V(3).inE('knows').outV().identity()
    gt = g.V(3).inE('knows', 'created')
    gt = g.V(4).inE('knows')
    gt = g.V(1).inE()

    gt = g.E(1).inV()
    gt = g.E(2, 4).inV()
    gt = g.E().inV()

    gt = g.V(1).outE().inV()

    gt = g.V(1).out('knows').identity()

    gt = g.V(4).both()
    gt = g.V(4).both('knows')
    gt = g.V(4).both('created')
    gt = g.V(1).both()
    gt = g.V(1).both('created')
    gt = g.V(1).both('creat')

    gt = g.V(4).bothE()
    gt = g.V(1).bothE('created')
    gt = g.V(3).bothE()
    gt = g.V(6).bothE('knows')

    gt = g.E(3).bothV()
    gt = g.E(1, 5, 6).bothV()

    gt = g.V().pack('p1')
    gt = g.unpackV('p1').identity()
    gt = g.V(2, 4, 5).pack('p2')
    gt = g.unpackV('p2').identity()

    # 表示非unused
    print(gt)
    print(t)

    print('Pass!')

    # profile.runctx("g.V().outE()", None, {'g':g})
    # t = timeit.timeit(lambda: g.V(), number=10000)
    # print('g.V()', t)
    # t = timeit.timeit(lambda: g.E(), number=10000)
    # print('g.E()', t)
    # t = timeit.timeit(lambda: g.V(1), number=10000)
    # print('g.V(1)', t)
    # t = timeit.timeit(lambda: g.E(1), number=10000)
    # print('g.E(1)', t)
    # t = timeit.timeit(lambda: g.V().outE(), number=10000)
    # print('g.V().outE()', t)
    # t = timeit.timeit(lambda: g.V().outE().inV(), number=10000)
    # print('g.V().outE().inV()', t)
    # t = timeit.timeit(lambda: g.V().hasLabel('person'), number=10000)
    # print("g.V().hasLabel('person')", t)

    #############################
    #         修改类功能         #
    #############################
    # g.addV()
    # g.addE(7, 6)
    # g.V(2).label('新的点二')
    # g.E(4).label('新的边四')

    #############################
    #          其他功能          #
    #############################
    # # 找出所有节点
    # g.V()
    # # 找出所有边
    # g.E()
    # # 找出所有人（label为person的节点）
    # g.V().hasLabel('person')
    # # 找出1号节点
    # g.V(1)
    # # 找出所有1号节点认识的人
    # g.V(1).out('knows')
    # # 找出一号节点参与过的软件开发的所有人员(包括自己)
    # g.V(1).out('created').in_('created')

def sqlite_mysql_read_write_test():
    # 连接数据库
    sqlite_conn = sqlite3.connect('gremlin-modern-graph.sqlite')
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

def main():
    sqlite_mysql_read_write_test()
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
    # sqlite_conn.close()

if __name__ == "__main__":
    t = time.time()
    main()
    elapsed = time.time() - t
    print('Time waste: {} => {}s'.format(datetime.timedelta(seconds=elapsed), elapsed))
