use parking_lot::{RwLock};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct TrieNode<T> {
    children: HashMap<char, Arc<RwLock<TrieNode<T>>>>,
    payload: Option<T>, // Payload to store additional data
}

impl<T> Deref for TrieNode<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.payload.as_ref().unwrap()
    }
}

impl<T> DerefMut for TrieNode<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.payload.as_mut().unwrap()
    }
}

impl<T> Default for TrieNode<T> {
    fn default() -> Self {
        TrieNode {
            children: HashMap::new(),
            payload: None,
        }
    }
}

pub struct SyncedTrie<T> {
    root: Arc<RwLock<TrieNode<T>>>,
}

// current impl is grow only
impl<T> SyncedTrie<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        SyncedTrie {
            root: Arc::new(RwLock::new(TrieNode::default())),
        }
    }

    // pub fn insert(&self, word: &str, payload: T) {
    //     let mut current_node = self.root.clone();
    //     for ch in word.chars() {
    //         let mut node = current_node.write();
    //         let child = node.children.entry(ch).or_default().clone();
    //         drop(node);
    //         current_node = child;
    //     }
    //     let mut node = current_node.write();
    //     node.payload = Some(payload);
    // }

    // don't guarantee the return node still exists after the function returns
    pub fn search_or_insert(
        &self,
        word: &str,
        payload: impl FnOnce() -> T,
    ) -> Arc<RwLock<TrieNode<T>>> {
        let mut current_node = self.root.clone();
        for ch in word.chars() {
            let mut node = current_node.write();
            let child = if let Some(child) = node.children.get(&ch) {
                child.clone()
            } else {
                // Create intermediate nodes without payload
                let new_inner_node = TrieNode::default();
                let new_child = Arc::new(RwLock::new(new_inner_node));
                let _ = node.children.insert(ch, new_child.clone());
                new_child
            };
            drop(node);
            current_node = child;
        }
        // Set payload only at the final node
        let mut final_node = current_node.write();
        if final_node.payload.is_none() {
            final_node.payload = Some(payload());
        }
        drop(final_node);
        current_node
    }

    // return (matchlen, node) array that is the prefix of the word
    pub fn match_partial(&self, word: &str) -> Vec<(usize, Arc<RwLock<TrieNode<T>>>)> {
        let mut current_node = self.root.clone();
        let mut nodes = Vec::new();
        let mut len = 1;
        for ch in word.chars() {
            let node = current_node.write();
            let child = if let Some(child) = node.children.get(&ch) {
                child.clone()
            } else {
                return nodes;
            };
            drop(node);
            current_node = child;

            if current_node.read().payload.is_some() {
                nodes.push((len, current_node.clone()));
            }
            len += 1;
        }
        nodes
    }
    // pub fn search(&self, word: &str) -> Option<Arc<RwLock<TrieNode<T>>>> {
    //     let mut current_node = self.root.clone();
    //     for ch in word.chars() {
    //         let node = current_node.write();
    //         let child = if let Some(child) = node.children.get(&ch) {
    //             child.clone()
    //         } else {
    //             return None;
    //         };
    //         drop(node);
    //         current_node = child;
    //     }
    //     Some(current_node)
    // }
}

/// commands to run:
/// cargo test --test test_basic_operations -- --nocapture
/// cargo test --test test_concurrent_insert -- --nocapture
/// cargo test --test test_concurrent_mixed_operations -- --nocapture
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let trie = SyncedTrie::new();
        let _ = trie.search_or_insert("test", || 42);
        let nodes = trie.match_partial("test");
        assert_eq!(nodes.len(), 1);
        assert_eq!(**nodes[0].1.read(), 42);
        assert!(trie.match_partial("none").is_empty());
    }

    #[test]
    fn test_concurrent_insert() {
        let trie = Arc::new(SyncedTrie::new());
        let mut handles = vec![];

        // 并发插入
        for i in 0..1000 {
            let trie = trie.clone();
            let key = format!("key{}", i);
            let value = i;
            handles.push(thread::spawn(move || {
                let _ = trie.search_or_insert(&key, || value);
            }));
        }

        // 等待所有插入完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 验证插入结果
        for i in 0..1000 {
            let key = format!("key{}", i);
            let node = trie.match_partial(&key);
            assert_eq!(node.len(), 1);
            assert_eq!(**node[0].1.read(), i);
        }
    }

    #[test]
    fn test_concurrent_mixed_operations() {
        let trie = Arc::new(SyncedTrie::new());
        let mut handles = vec![];

        // 预先插入一些数据
        for i in 0..10 {
            let _ = trie.search_or_insert(&format!("key{}", i), || i);
        }

        // 混合读写操作
        for i in 0..1000 {
            let trie = trie.clone();
            handles.push(thread::spawn(move || {
                let key = format!("key{}", i % 10);
                if i % 3 == 0 {
                    let _ = trie.search_or_insert(&key, || i);
                } else {
                    let nodes = trie.match_partial(&key);
                    assert_eq!(nodes.len(), 1);
                    assert_eq!(**nodes[0].1.read(), i);
                }
            }));
        }

        // 等待所有操作完成
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_prefix_operations() {
        let trie = Arc::new(SyncedTrie::new());
        let mut handles = vec![];

        // 测试相同前缀的并发操作
        for i in 0..100 {
            let trie = trie.clone();
            handles.push(thread::spawn(move || {
                let key = format!("prefix{}", i % 10);
                let _ = trie.search_or_insert(&key, || i);
                let nodes = trie.match_partial(&key);
                assert_eq!(nodes.len(), 1);
                assert_eq!(**nodes[0].1.read(), i);
            }));
        }

        // 等待所有操作完成
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_match_partial() {
        let trie = SyncedTrie::new();

        // Insert some test data
        let _ = trie.search_or_insert("test", || 1);
        let _ = trie.search_or_insert("testing", || 2);
        let _ = trie.search_or_insert("te", || 3);

        // Test partial matches
        let matches = trie.match_partial("testing");
        assert_eq!(matches.len(), 1); // Should get the node for "testing"
        assert_eq!(**matches.last().unwrap().1.read(), 2);

        // Test prefix
        let matches = trie.match_partial("te");
        assert_eq!(matches.len(), 1); // Should get the node for "te"
        assert_eq!(**matches.last().unwrap().1.read(), 3);

        // Test non-existent prefix
        let matches = trie.match_partial("xyz");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_match_partial_empty_nodes() {
        let trie = SyncedTrie::new();

        // Insert a long word
        let _ = trie.search_or_insert("hello", || 1);

        // Test that intermediate nodes don't have payloads
        let matches = trie.match_partial("h");
        assert!(matches.is_empty()); // No payload at 'h'

        let matches = trie.match_partial("hel");
        assert!(matches.is_empty()); // No payload at 'hel'

        // But full match should work
        let matches = trie.match_partial("hello");
        assert_eq!(matches.len(), 1);
        assert_eq!(**matches.last().unwrap().1.read(), 1);
    }
}
