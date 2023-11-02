// pub struct LocalKVSled {}

// impl LocalKV for LocalKVSled {
//     fn new() -> LocalKVSled {
//         LocalKVSled {}
//     }

//     fn get(&self, key: &str) -> Option<String> {
//         let db = sled::open("my_db").unwrap();
//         let result = db.get(key);
//         match result {
//             Ok(Some(value)) => {
//                 let value = value.to_vec();
//                 let value = String::from_utf8(value).unwrap();
//                 Some(value)
//             }
//             _ => None,
//         }
//     }

//     fn set(&self, key: &str, value: &str) -> bool {
//         let db = sled::open("my_db").unwrap();
//         let result = db.insert(key, value);
//         match result {
//             Ok(_) => true,
//             _ => false,
//         }
//     }

//     fn delete(&self, key: &str) -> bool {
//         let db = sled::open("my_db").unwrap();
//         let result = db.remove(key);
//         match result {
//             Ok(_) => true,
//             _ => false,
//         }
//     }
// }
