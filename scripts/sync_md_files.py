#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

class Node:
    def __init__(self, data: dict):
        self.data = data
        self.children = []
        self.parent = None
        
    @property
    def id(self) -> str:
        return self.data.get('id', '')
        
    @property
    def type(self) -> str:
        return self.data.get('type', '')
        
    @property
    def x(self) -> float:
        return float(self.data.get('x', 0))
        
    @property
    def y(self) -> float:
        return float(self.data.get('y', 0))
        
    @property
    def width(self) -> float:
        return float(self.data.get('width', 0))
        
    @property
    def height(self) -> float:
        return float(self.data.get('height', 0))
        
    def contains(self, other: 'Node') -> bool:
        """判断当前节点是否在空间上包含另一个节点"""
        if self.type != 'group':
            return False
            
        # 考虑边界重叠的情况
        return (other.x >= self.x - 1 and
                other.y >= self.y - 1 and
                other.x + other.width <= self.x + self.width + 1 and
                other.y + other.height <= self.y + self.height + 1)
                
    def to_dict(self) -> dict:
        """转换为字典格式"""
        result = self.data.copy()
        if self.children:
            result['children'] = [child.to_dict() for child in self.children]
        return result
        
    def to_flat_dict(self) -> List[dict]:
        """转换为扁平的字典列表"""
        result = []
        if self.type != 'root':  # 不包含根节点
            node_data = self.data.copy()
            if 'children' in node_data:
                del node_data['children']  # 移除children字段
            result.append(node_data)
        for child in self.children:
            result.extend(child.to_flat_dict())
        return result

def tree_to_flat_nodes(tree_data: dict) -> List[dict]:
    """将树状结构转换为扁平的节点列表"""
    result = []
    
    # 处理当前节点
    if tree_data.get('type') != 'root':
        node_data = tree_data.copy()
        if 'children' in node_data:
            del node_data['children']
        result.append(node_data)
    
    # 递归处理子节点
    for child in tree_data.get('children', []):
        result.extend(tree_to_flat_nodes(child))
        
    return result

class CanvasData:
    def __init__(self, data: dict):
        self.nodes = []
        self.groups = []
        self.edges = []
        self.parse_data(data)
        
    def parse_data(self, data: dict):
        """解析canvas数据"""
        # 处理所有节点
        for item in data:
            node = Node(item)
            self.nodes.append(node)
            if node.type == 'group':
                self.groups.append(node)
                
    def find_best_parent(self, node: Node) -> Optional[Node]:
        """为节点找到最佳的父节点"""
        candidates = []
        for group in self.groups:
            if group.contains(node) and group != node:
                candidates.append(group)
                
        if not candidates:
            return None
            
        # 选择面积最小的包含组作为父节点
        return min(candidates, 
                  key=lambda g: g.width * g.height)
                
    def build_tree(self) -> Node:
        """构建树状结构"""
        # 创建虚拟根节点
        root = Node({
            'id': 'root',
            'type': 'root',
        })
        
        # 按面积从大到小排序groups
        self.groups.sort(key=lambda g: g.width * g.height, reverse=True)
        
        # 构建节点关系
        assigned_nodes = set()
        
        # 先处理groups之间的关系
        for group in self.groups:
            parent = self.find_best_parent(group)
            if parent:
                parent.children.append(group)
                group.parent = parent
                assigned_nodes.add(group.id)
            else:
                root.children.append(group)
                group.parent = root
                assigned_nodes.add(group.id)
                
        # 处理剩余节点
        for node in self.nodes:
            if node.id not in assigned_nodes:
                parent = self.find_best_parent(node)
                if parent:
                    parent.children.append(node)
                    node.parent = parent
                else:
                    root.children.append(node)
                    node.parent = root
                    
        return root
        
    def to_tree_json(self) -> dict:
        """转换为树状JSON结构"""
        root = self.build_tree()
        return root.to_dict()
        
    def to_flat_json(self) -> List[dict]:
        """转换为扁平JSON结构"""
        root = self.build_tree()
        return root.to_flat_dict()

def backup_file(file_path: str):
    """备份文件"""
    if os.path.exists(file_path):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        backup_path = f"{file_path}.{timestamp}.bak"
        os.rename(file_path, backup_path)
        print(f"Backup {file_path} to {backup_path}")

def sync_from_s3fs():
    """从s3fs同步到本地，并生成树状结构"""
    s3fs_dir = "/mnt/s3fs/waverless"
    local_dir = "/root/prjs/waverless"
    
    print(f"Starting sync from {s3fs_dir} to {local_dir}")
    
    # 同步canvas文件
    canvas_path = os.path.join(local_dir, "design.canvas")
    s3fs_canvas_path = os.path.join(s3fs_dir, "design.canvas")
    
    if os.path.exists(s3fs_canvas_path):
        # 备份当前文件
        backup_file(canvas_path)
        
        # 读取s3fs中的canvas
        with open(s3fs_canvas_path, 'r', encoding='utf-8') as f:
            canvas_data = json.load(f)
            
        # 生成树状结构
        canvas = CanvasData(canvas_data.get('nodes', []))
        tree_data = canvas.to_tree_json()
        
        # 保存树状结构
        tree_path = os.path.join(local_dir, "design.json")
        with open(tree_path, 'w', encoding='utf-8') as f:
            json.dump(tree_data, f, ensure_ascii=False, indent=2)
            
        # 保存原始canvas
        with open(canvas_path, 'w', encoding='utf-8') as f:
            json.dump(canvas_data, f, ensure_ascii=False, indent=2)
            
def sync_to_s3fs():
    """从本地同步到s3fs，将树状结构转换回扁平结构"""
    s3fs_dir = "/mnt/s3fs/waverless"
    local_dir = "/root/prjs/waverless"
    
    print(f"Starting sync from {local_dir} to {s3fs_dir}")
    
    # 读取树状结构
    tree_path = os.path.join(local_dir, "design.json")
    if not os.path.exists(tree_path):
        print(f"Tree file {tree_path} not found")
        return
        
    with open(tree_path, 'r', encoding='utf-8') as f:
        tree_data = json.load(f)
        
    # 直接将树状结构转换为扁平节点列表
    flat_nodes = tree_to_flat_nodes(tree_data)
    
    # 保存到s3fs
    s3fs_canvas_path = os.path.join(s3fs_dir, "design.canvas")
    backup_file(s3fs_canvas_path)
    
    with open(s3fs_canvas_path, 'w', encoding='utf-8') as f:
        json.dump({'nodes': flat_nodes}, f, ensure_ascii=False, indent=2)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 sync_md_files.py [from_s3fs|to_s3fs]")
        sys.exit(1)
        
    command = sys.argv[1]
    if command == "from_s3fs":
        sync_from_s3fs()
    elif command == "to_s3fs":
        sync_to_s3fs()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()
