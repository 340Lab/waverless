#!/usr/bin/env python3
import os
import json
import shutil
import unittest
from scripts.design_json_tool import DesignJson, Node

class TestDesignJsonTool(unittest.TestCase):
    def setUp(self):
        """测试前准备工作"""
        # 创建测试用的JSON文件
        self.test_json_path = 'test_design.json'
        self.test_data = {
            "id": "root",
            "type": "root",
            "children": [
                {
                    "id": "group1",
                    "type": "group",
                    "label": "测试组1",
                    "children": [
                        {
                            "id": "node1",
                            "type": "text",
                            "text": "测试节点1"
                        }
                    ]
                }
            ],
            "edges": []
        }
        with open(self.test_json_path, 'w', encoding='utf-8') as f:
            json.dump(self.test_data, f, ensure_ascii=False, indent=2)
        
        self.design = DesignJson(self.test_json_path)
        
    def tearDown(self):
        """测试后清理工作"""
        if os.path.exists(self.test_json_path):
            os.remove(self.test_json_path)
            
    def test_read_all(self):
        """测试读取整个JSON"""
        root = self.design.root
        self.assertEqual(root.id, "root")
        self.assertEqual(root.type, "root")
        self.assertEqual(len(root.children), 1)
        
    def test_read_node(self):
        """测试读取单个节点"""
        node = self.design.get_node("node1")
        self.assertIsNotNone(node)
        self.assertEqual(node.type, "text")
        self.assertEqual(node.text, "测试节点1")
        
    def test_read_group(self):
        """测试读取组内容"""
        nodes = self.design.get_group_nodes("group1")
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "node1")
        
    def test_create_node(self):
        """测试创建新节点"""
        node_data = {
            "id": "new_node",
            "type": "text",
            "text": "新建节点"
        }
        node_id = self.design.create_node(node_data)
        self.assertEqual(node_id, "new_node")
        node = self.design.get_node(node_id)
        self.assertIsNotNone(node)
        self.assertEqual(node.text, "新建节点")
        
    def test_update_node(self):
        """测试更新节点"""
        updates = {"text": "更新后的文本"}
        success = self.design.update_node("node1", updates)
        self.assertTrue(success)
        node = self.design.get_node("node1")
        self.assertEqual(node.text, "更新后的文本")
        
    def test_move_to_group(self):
        """测试移动节点到组"""
        # 先创建新组
        group_data = {
            "id": "group2",
            "type": "group",
            "label": "测试组2"
        }
        self.design.create_node(group_data)
        
        # 移动节点
        success = self.design.move_to_group("node1", "group2")
        self.assertTrue(success)
        
        # 验证移动结果
        nodes = self.design.get_group_nodes("group2")
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "node1")
        
    def test_edges(self):
        """测试边操作"""
        # 添加边
        success = self.design.add_edge("node1", "group1", "test_edge")
        self.assertTrue(success)
        
        # 验证入度
        incoming = self.design.get_incoming_nodes("group1")
        self.assertEqual(len(incoming), 1)
        self.assertEqual(incoming[0], ("node1", "test_edge"))
        
        # 验证出度
        outgoing = self.design.get_outgoing_nodes("node1")
        self.assertEqual(len(outgoing), 1)
        self.assertEqual(outgoing[0], ("group1", "test_edge"))
        
        # 删除边
        success = self.design.remove_edge("node1", "group1", "test_edge")
        self.assertTrue(success)
        
        # 验证边已删除
        incoming = self.design.get_incoming_nodes("group1")
        self.assertEqual(len(incoming), 0)

    def test_nonexistent_node(self):
        """测试操作不存在的节点"""
        # 读取不存在的节点
        node = self.design.get_node("nonexistent")
        self.assertIsNone(node)
        
        # 更新不存在的节点
        success = self.design.update_node("nonexistent", {"text": "新文本"})
        self.assertFalse(success)
        
        # 移动不存在的节点
        success = self.design.move_to_group("nonexistent", "group1")
        self.assertFalse(success)
        
        # 添加包含不存在节点的边
        success = self.design.add_edge("nonexistent", "node1")
        self.assertFalse(success)

    def test_duplicate_operations(self):
        """测试重复操作"""
        # 重复创建同ID节点
        node_data = {
            "id": "node1",  # 已存在的ID
            "type": "text",
            "text": "重复节点"
        }
        original_node = self.design.get_node("node1")
        node_id = self.design.create_node(node_data)
        self.assertEqual(node_id, "node1")
        # 验证节点内容未被覆盖
        node = self.design.get_node("node1")
        self.assertEqual(node.text, original_node.text)
        
        # 重复添加相同的边
        self.design.add_edge("node1", "group1", "test_edge")
        success = self.design.add_edge("node1", "group1", "test_edge")
        self.assertTrue(success)  # 添加成功但不会重复
        incoming = self.design.get_incoming_nodes("group1")
        self.assertEqual(len(incoming), 1)  # 只有一条边

    def test_nested_groups(self):
        """测试嵌套组操作"""
        # 创建嵌套的组结构
        group2_data = {
            "id": "group2",
            "type": "group",
            "label": "测试组2"
        }
        group3_data = {
            "id": "group3",
            "type": "group",
            "label": "测试组3"
        }
        self.design.create_node(group2_data)
        self.design.create_node(group3_data)
        
        # 将group3移动到group2中
        success = self.design.move_to_group("group3", "group2")
        self.assertTrue(success)
        
        # 验证嵌套结构
        nodes = self.design.get_group_nodes("group2")
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "group3")
        
        # 将节点移动到最内层组
        success = self.design.move_to_group("node1", "group3")
        self.assertTrue(success)
        
        # 验证节点位置
        nodes = self.design.get_group_nodes("group3")
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "node1")

    def test_save_and_load(self):
        """测试保存和加载功能"""
        # 修改数据
        self.design.update_node("node1", {"text": "修改后的文本"})
        self.design.add_edge("node1", "group1", "test_edge")
        
        # 保存文件
        self.design.save()
        
        # 重新加载
        new_design = DesignJson(self.test_json_path)
        
        # 验证修改是否保持
        node = new_design.get_node("node1")
        self.assertEqual(node.text, "修改后的文本")
        
        incoming = new_design.get_incoming_nodes("group1")
        self.assertEqual(len(incoming), 1)
        self.assertEqual(incoming[0], ("node1", "test_edge"))

    def test_invalid_operations(self):
        """测试无效操作"""
        # 测试移动到非组节点
        success = self.design.move_to_group("node1", "node1")  # node1不是组
        self.assertFalse(success)
        
        # 测试更新不存在的属性
        success = self.design.update_node("node1", {"nonexistent_attr": "value"})
        self.assertTrue(success)  # 更新成功但属性未添加
        node = self.design.get_node("node1")
        self.assertFalse(hasattr(node, "nonexistent_attr"))
        
        # 测试创建缺少必要属性的节点
        invalid_node = {
            "type": "text"  # 缺少id
        }
        with self.assertRaises(KeyError):
            self.design.create_node(invalid_node)

if __name__ == '__main__':
    unittest.main() 