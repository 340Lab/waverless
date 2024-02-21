use crate::r#struct::*;
use crate::*;
use wasm_serverless_lib::*;

struct NotesMan {}
impl NotesMan {
    pub fn new() -> Self {
        Self {}
    }
    pub fn set_notelist(&self, list: Vec<Value>) {
        // self.kernel.set("notes_meta".to_string(),serde_json::to_string(&list).unwrap());
        KvBatch::new()
            .then_set(
                "notes_meta".as_bytes(),
                serde_json::to_string(&list).unwrap().as_bytes(),
            )
            .finally_call();
    }
    pub fn get_note_list(&self) -> Vec<Value> {
        // let s = self.kernel.get("notes_meta".to_string());
        let s = KvBatch::new()
            .then_get("notes_meta".as_bytes())
            .finally_call();
        let s = s.res_str();
        let mut node_id_name_list: Vec<serde_json::Value> = Vec::new();
        if s.is_none() {
            KvBatch::new()
                .then_set(
                    "notes_meta".as_bytes(),
                    serde_json::to_string(&node_id_name_list)
                        .unwrap()
                        .as_bytes(),
                )
                .finally_call();
            // self.kernel.set(
            //     "notes_meta".to_string(),
            //     serde_json::to_string(&node_id_name_list).unwrap(),
            // );
        } else if let Some(s) = s {
            // println!("saved data {}",s);
            node_id_name_list = serde_json::from_str(&*s).unwrap();
        }
        node_id_name_list
    }
}

impl ListApi for Impl {
    fn get_notes_mata(&self, req: GetNotesMataReq) -> GetNotesMataResp {
        let node_id_name_list = NotesMan::new().get_note_list();
        GetNotesMataResp { node_id_name_list }
    }

    fn create_new_note(&self, req: CreateNewNoteReq) -> CreateNewNoteResp {
        let man = NotesMan::new();
        let mut nl = man.get_note_list();
        let mut one_id_to_name = Vec::new();
        one_id_to_name.reserve(2);
        one_id_to_name.push(Value::String(nl.len().to_string()));
        one_id_to_name.push(Value::String("新的笔记".to_string()));
        nl.push(Value::Array(one_id_to_name));
        man.set_notelist(nl);
        CreateNewNoteResp {}
    }

    fn rename_note(&self, arg: RenameNoteReq) -> RenameNoteResp {
        let man = NotesMan::new();

        let mut notelist = man.get_note_list();
        for id_name in &mut notelist {
            let id_name = id_name.as_array_mut().unwrap();
            if id_name[0].as_str().unwrap() == arg.noteid {
                id_name[1] = Value::String(arg.name);
                break;
            }
        }
        man.set_notelist(notelist);

        RenameNoteResp {}
    }
}
