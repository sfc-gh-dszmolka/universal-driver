use std::collections::HashMap;

pub enum Setting {
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

pub struct Connection {
    pub settings: HashMap<String, Setting>,
}

impl Connection {
    pub fn new() -> Self {
        Connection { settings: HashMap::new() }
    }
}

pub struct Database {
    pub settings: HashMap<String, Setting>,
}

impl Database {
    pub fn new() -> Self {
        Database { settings: HashMap::new() }
    }
}

pub struct Statement {
    pub settings: HashMap<String, Setting>,
}

impl Statement {
    pub fn new() -> Self {
        Statement { settings: HashMap::new() }
    }
}
