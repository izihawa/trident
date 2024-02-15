use crate::config::ShardConfig;
use bisection::bisect_right;
use std::collections::HashMap;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{Hash, Hasher};

impl Hash for ShardConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state)
    }
}

#[derive(Clone)]
pub struct HashRing {
    v_nodes: usize,
    ring: HashMap<u128, ShardConfig>,
    sorted_keys: Vec<u128>,
}

impl HashRing {
    pub fn with_hasher<'a>(nodes: impl Iterator<Item = &'a ShardConfig>) -> HashRing {
        let mut new_hash_ring = HashRing {
            v_nodes: 160,
            ring: HashMap::new(),
            sorted_keys: Vec::new(),
        };
        new_hash_ring.add_nodes(nodes);
        new_hash_ring
    }

    /// Adds a node to the hash ring
    pub fn add_nodes<'a>(&mut self, nodes: impl Iterator<Item = &'a ShardConfig>) {
        for node in nodes {
            for i in 0..self.v_nodes * node.weight {
                let node_name = format!("{}-{}", &node.name, i);
                let node_key = self.gen_key(&node_name);
                self.ring.insert(node_key, node.clone());
                self.sorted_keys.push(node_key);
            }
        }
        self.sorted_keys = BinaryHeap::from(self.sorted_keys.clone()).into_sorted_vec();
    }

    pub fn range(&self, key: &str, size: usize) -> Vec<&ShardConfig> {
        let mut result = Vec::with_capacity(size);
        let mut visited = HashSet::new();
        let position = if let Some(position) = self.get_pos(key) {
            position
        } else {
            return result;
        };

        for current_position in position..position + self.sorted_keys.len() {
            let node = &self.ring[&self.sorted_keys[current_position % self.sorted_keys.len()]];
            if visited.insert(node) {
                result.push(node)
            }
            if result.len() == size {
                return result;
            }
        }
        result
    }

    fn get_pos(&self, key: &str) -> Option<usize> {
        if self.sorted_keys.is_empty() {
            return None;
        }
        let generated_key = self.gen_key(key);
        let position = bisect_right(&self.sorted_keys, &generated_key);
        Some(position % self.sorted_keys.len())
    }

    /// Generates a key from a string value
    fn gen_key(&self, key: &str) -> u128 {
        let digest = md5::compute(key.as_bytes());
        u128::from_be_bytes(*digest)
    }
}
