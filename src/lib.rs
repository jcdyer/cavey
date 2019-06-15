use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Cavey {
    map: BTreeMap<String, String>,
}

impl Cavey {
    pub fn new() -> Cavey {
        Cavey::default()
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).map(|x| x.to_owned())
    }

    pub fn put(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
    pub fn keys(&mut self) -> Vec<String> {
        self.map.keys().cloned().collect()
    }
}
