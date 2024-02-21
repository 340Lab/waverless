use crate::r#struct::*;
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize)]
struct ArticleListNode {
    pub barid: String,
    pub artname: String,
}
#[derive(Serialize, Deserialize, Default)]
struct ArticleList {
    pub list: Vec<ArticleListNode>,
}
impl ArticleList {
    pub fn bind(&mut self, arg: ArticleBinderReq) -> bool {
        for a in &self.list {
            if a.barid == arg.barid {
                //文章已绑定
                self.rename(arg);
                return true;
            }
        }
        self.list.push(ArticleListNode {
            barid: arg.barid,
            artname: arg.article_name,
        });
        true
    }
    pub fn unbind(&mut self, arg: ArticleBinderReq) -> bool {
        let oldsz = self.list.len();
        self.list.retain(|v| v.barid != arg.article_name);
        oldsz != self.list.len()
    }
    pub fn rename(&mut self, mut arg: ArticleBinderReq) -> bool {
        for a in &mut self.list {
            if a.barid == arg.barid {
                std::mem::swap(&mut a.artname, &mut arg.article_name);
                return true;
            }
        }
        return false;
    }
}

#[derive(Serialize, Deserialize)]
struct PathInfo {
    pathtype: i32,
}

#[derive(Serialize, Deserialize)]
pub struct NoteBarInfo {
    pub x: f32,
    pub y: f32,
    pub w: f32,
    pub h: f32,
    pub text: String,
    pub formatted: String,
    pub connected: Vec<serde_json::Value>,
    pub edit_time: Option<u64>,
    pub create_time: Option<u64>,
    pub epoch: Option<u64>,
}
impl NoteBarInfo {
    pub fn epoch_i32(&self) -> i32 {
        self.epoch.unwrap() as i32
    }
    pub fn epoch_addup(&mut self) {
        if self.epoch.unwrap() == i32::MAX as u64 {
            self.epoch.replace(0);
        } else {
            let old = self.epoch.unwrap();
            self.epoch.replace(old + 1);
        }
    }
    pub fn fix_old(&mut self) {
        if self.epoch.is_none() {
            self.epoch = Some(0);
        }
    }
    pub fn update_edit_time(&mut self) {
        self.edit_time = Some(time_stamp_ms_u64());
    }
    pub fn to_reply(self) -> GetNoteBarInfoResp {
        GetNoteBarInfoResp {
            x: self.x,
            y: self.y,
            w: self.w,
            h: self.h,
            text: self.text,
            formatted: self.formatted,
            connected: self.connected,
            epoch: self.epoch.unwrap() as i32,
        }
    }
    pub fn remove_path(&mut self, pathid: &str) {
        self.connected.retain(|s| s.as_str().unwrap() != pathid)
    }
}

const CHUNK_W: i32 = 300;
const CHUNK_H: i32 = 400;

pub fn time_stamp_ms_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn chunkpos_to_chunkid(x: i32, y: i32) -> String {
    format!("{}_{}", x, y)
}

fn note_pos_to_chunkpos(x: f32, y: f32) -> (i32, i32) {
    (
        if x > 0.0 {
            x as i32 / CHUNK_W
        } else {
            x as i32 / CHUNK_W - 1
        },
        if y > 0.0 {
            y as i32 / CHUNK_H
        } else {
            y as i32 / CHUNK_H - 1
        },
    )
}

fn fitup_chunk_range_in_notemeta_for_newck(
    ckx: i32,
    cky: i32,
    meta_: &mut GetNoteMataResp,
) -> bool {
    let mut changed = false;
    if meta_.max_chunkx < ckx {
        meta_.max_chunkx = ckx;
        changed = true;
    } else if meta_.min_chunkx > ckx {
        meta_.min_chunkx = ckx;
        changed = true;
    }
    if meta_.max_chunky < cky {
        meta_.max_chunky = cky;
        changed = true;
    } else if meta_.min_chunky > cky {
        meta_.min_chunky = cky;
        changed = true;
    }

    changed
}

struct NoteMan {}
impl NoteMan {
    fn new() -> Self {
        Self {}
    }

    pub fn fitdown_chunk_range_in_notemeta_if_ck_is_on_edge(
        &self,
        noteid: &str,
        ckx: i32,
        cky: i32,
        meta_: &mut GetNoteMataResp,
    ) -> bool {
        fn y_empty(man: &NoteMan, x: i32, meta_: &GetNoteMataResp, noteid: &str) -> bool {
            let mut line_empty = true;
            for y in meta_.min_chunky..meta_.max_chunky + 1 {
                if man
                    .get_note_chunk_ids(GetChunkNoteIdsReq {
                        noteid: noteid.to_string(),
                        chunkx: x,
                        chunky: y,
                    })
                    .noteids
                    .len()
                    != 0
                {
                    //如果有非空的chunk
                    line_empty = false;
                    break;
                }
            }
            line_empty
        };
        fn x_empty(man: &NoteMan, y: i32, meta_: &GetNoteMataResp, noteid: &str) -> bool {
            let mut line_empty = true;
            for x in meta_.min_chunkx..meta_.max_chunkx + 1 {
                if man
                    .get_note_chunk_ids(GetChunkNoteIdsReq {
                        noteid: noteid.to_string(),
                        chunkx: x,
                        chunky: y,
                    })
                    .noteids
                    .len()
                    != 0
                {
                    //如果有非空的chunk
                    line_empty = false;
                    break;
                }
            }
            line_empty
        };
        let mut changed = false;
        if ckx == meta_.max_chunkx {
            let mut new_max_chunkx = meta_.max_chunkx;
            //从大到小压缩
            for x in (1..meta_.max_chunkx + 1).rev() {
                if y_empty(self, x, meta_, noteid) {
                    new_max_chunkx = x - 1;
                } else {
                    break;
                }
            }
            if meta_.max_chunkx != new_max_chunkx {
                changed = true;
            }
            meta_.max_chunkx = new_max_chunkx;
        } else if ckx == meta_.min_chunkx {
            let mut new_min_chunkx = meta_.min_chunkx;
            //从xiao到大压缩
            for x in meta_.min_chunkx..0 {
                if y_empty(self, x, meta_, noteid) {
                    new_min_chunkx = x + 1;
                } else {
                    break;
                }
            }
            if meta_.min_chunkx != new_min_chunkx {
                changed = true
            }
            meta_.min_chunkx = new_min_chunkx;
        }
        if cky == meta_.max_chunky {
            let mut new_max_chunky = meta_.max_chunky;
            //从大到小压缩
            for y in (1..meta_.max_chunky + 1).rev() {
                if x_empty(self, y, meta_, noteid) {
                    new_max_chunky = y - 1;
                } else {
                    break;
                }
            }
            if meta_.max_chunky != new_max_chunky {
                changed = true;
            }
            meta_.max_chunky = new_max_chunky;
        } else if cky == meta_.min_chunky {
            let mut new_min_chunky = meta_.min_chunky;
            //从xiao到大压缩
            for y in meta_.min_chunky..0 {
                if x_empty(self, y, meta_, noteid) {
                    new_min_chunky = y + 1;
                } else {
                    break;
                }
            }
            if meta_.min_chunky != new_min_chunky {
                changed = true;
            }
            meta_.min_chunky = new_min_chunky;
        }
        changed
    }

    pub fn set_note_meta(&self, noteid: &str, meta: &GetNoteMataResp) {
        // self.kernel.set(
        //     format!("{}|meta", noteid),
        //     serde_json::to_string(meta).unwrap(),
        // );
        KvBatch::new()
            .then_set(
                format!("{}|meta", noteid).as_bytes(),
                serde_json::to_string(meta).unwrap().as_bytes(),
            )
            .finally_call();
    }
    fn get_note_meta(&self, noteid: &str) -> GetNoteMataResp {
        let s = KvBatch::new()
            .then_get(format!("{}|meta", noteid).as_bytes())
            .finally_call();
        let s = s.res_str();
        match s {
            None => {
                let ret = GetNoteMataResp {
                    next_noteid: 0,
                    max_chunkx: 0,
                    max_chunky: 0,
                    min_chunkx: 0,
                    min_chunky: 0,
                };
                KvBatch::new()
                    .then_set(
                        format!("{}|meta", noteid).as_bytes(),
                        serde_json::to_string(&ret).unwrap().as_bytes(),
                    )
                    .finally_call();
                ret
            }
            Some(s) => serde_json::from_str::<GetNoteMataResp>(&s).unwrap(),
        }
    }

    fn get_note_chunk_ids(&self, arg: GetChunkNoteIdsReq) -> GetChunkNoteIdsResp {
        // let chunkid = chunkpos_to_chunkid(arg.chunkx, arg.chunky);
        // {//try read from mem
        //     let mut hold = self.locked_data.write();
        //     let noteinfo = hold.get_note_info(connid, &*arg.noteid);
        //     if let Some(v) = noteinfo.get_chunk_noteids(&*chunkid) {
        //         return GetChunkNoteIdsReply {
        //             noteids: v.collect_vec_value()
        //         };
        //     }
        // }

        //mem no, load from kv
        let key = format!("{}|chunk|{}|{}", arg.noteid, arg.chunkx, arg.chunky);
        let s = KvBatch::new().then_get(key.as_bytes()).finally_call();
        let s = s.res_str();
        let kvres = match s {
            None => {
                let ret = GetChunkNoteIdsResp {
                    noteids: Vec::new(),
                };
                // self.kernel.set(key, serde_json::to_string(&ret).unwrap());
                KvBatch::new()
                    .then_set(
                        key.as_bytes(),
                        serde_json::to_string(&ret).unwrap().as_bytes(),
                    )
                    .finally_call();
                ret
            }
            Some(s) => serde_json::from_str::<GetChunkNoteIdsResp>(&s).unwrap(),
        };
        // //save to mem store
        // {
        //     let mut hold = self.locked_data.write();
        //     let noteinfo = hold.get_note_info(connid, &*arg.noteid);
        //     noteinfo.set_chunk_noteids(&*chunkid, kvres.noteids.clone());
        // }
        return kvres;
    }
    pub fn set_note_chunk_ids(
        &self,
        // connid: i32,
        noteid: &str,
        cx: i32,
        cy: i32,
        ids: &GetChunkNoteIdsResp,
    ) {
        let chunkid = chunkpos_to_chunkid(cx, cy);
        // {
        //     //mem
        //     let mut hold = self.locked_data.write();
        //     let noteinfo = hold.get_note_info(connid, noteid);
        //     noteinfo.set_chunk_noteids(&*chunkid, ids.noteids.clone());
        // }
        //kv
        let key = format!("{}|chunk|{}|{}", noteid, cx, cy);
        // self.kernel.set(key, serde_json::to_string(ids).unwrap());
        KvBatch::new()
            .then_set(
                key.as_bytes(),
                serde_json::to_string(ids).unwrap().as_bytes(),
            )
            .finally_call();
    }
    //变化完后需要检查是否发生收缩，
    // return 区块范围
    pub fn change_notebar_chunk(
        &self,
        noteid: &str,
        barid: &str,
        ck1: (i32, i32),
        ck2: (i32, i32),
    ) -> GetNoteMataResp {
        //改变两个chunk中的noteids
        //chunk1中移除
        let mut ck1data = self.get_note_chunk_ids(GetChunkNoteIdsReq {
            noteid: noteid.to_string(),
            chunkx: ck1.0,
            chunky: ck1.1,
        });
        let mut found = ck1data.noteids.len();
        for i in 0..ck1data.noteids.len() {
            if ck1data.noteids[i] == barid {
                found = i
            }
        }
        ck1data.noteids.remove(found);

        //chunk2中放入
        let mut ck2data = self.get_note_chunk_ids(GetChunkNoteIdsReq {
            noteid: noteid.to_string(),
            chunkx: ck2.0,
            chunky: ck2.1,
        });
        ck2data
            .noteids
            .push(serde_json::Value::String(barid.to_string()));
        self.set_note_chunk_ids(noteid, ck1.0, ck1.1, &ck1data);
        self.set_note_chunk_ids(noteid, ck2.0, ck2.1, &ck2data);

        let mut meta = self.get_note_meta(noteid);
        let mut range_changed = fitup_chunk_range_in_notemeta_for_newck(ck2.0, ck2.1, &mut meta);
        range_changed = self
            .fitdown_chunk_range_in_notemeta_if_ck_is_on_edge(noteid, ck1.0, ck1.1, &mut meta)
            || range_changed;
        if range_changed {
            self.set_note_meta(noteid, &meta);
        }
        meta
    }
    pub fn set_notebar_info(&self, noteid: &str, barid: &str, newinfo: &NoteBarInfo) {
        let key = format!("{}|bar|{}", noteid, barid);
        // println!("get_notebar_info {}",key);
        KvBatch::new()
            .then_set(
                key.as_bytes(),
                serde_json::to_string(newinfo).unwrap().as_bytes(),
            )
            .finally_call();
    }
    pub fn get_notebar_info(&self, noteid: String, barid: String) -> Option<NoteBarInfo> {
        let key = format!("{}|bar|{}", noteid, barid);
        println!("get_notebar_info {}", key);
        let s = KvBatch::new().then_get(key.as_bytes()).finally_call();
        let s = s.res_str();
        //create if not exist
        match s {
            None => None,
            Some(s) => {
                let mut fix = serde_json::from_str::<NoteBarInfo>(&s).unwrap();
                fix.fix_old();
                Some(fix)
            }
        }
    }
    pub fn create_notebar(
        &self,
        x: f32,
        y: f32,
        noteid: String,
    ) -> (GetNoteBarInfoResp, (i32, i32)) {
        //1.读取元数据
        let mut meta = self.get_note_meta(&*noteid);
        let next = meta.next_noteid;
        meta.next_noteid += 1;
        let ret = {
            //notebar 信息
            let time = time_stamp_ms_u64();
            let ret = NoteBarInfo {
                x,
                y,
                w: 150.0,
                h: 150.0,
                text: "".to_string(),
                formatted: "".to_string(),
                connected: vec![],
                edit_time: Some(time),
                create_time: Some(time),
                epoch: Some(0),
            }; //保存笔记信息
            let key = format!("{}|bar|{}", noteid, next);
            KvBatch::new()
                .then_set(
                    key.as_bytes(),
                    serde_json::to_string(&ret).unwrap().as_bytes(),
                )
                .finally_call();
            ret.to_reply()
        };

        let (cx, cy) = note_pos_to_chunkpos(x, y);
        {
            // chunk ids 信息
            let mut chunk_noteids = self.get_note_chunk_ids(GetChunkNoteIdsReq {
                noteid: noteid.clone(),
                chunkx: cx,
                chunky: cy,
            });
            chunk_noteids.noteids.push(Value::String(next.to_string()));
            self.set_note_chunk_ids(&noteid, cx, cy, &chunk_noteids);
        }

        //meta fit 一下chunk
        fitup_chunk_range_in_notemeta_for_newck(cx, cy, &mut meta);
        self.set_note_meta(&*noteid, &meta);

        (ret, (cx, cy))
    }
    pub fn delete_bar(&self, noteid: &str, ebid: &str) -> GetNoteMataResp {
        let mut meta = self.get_note_meta(noteid);
        if let Some(mut n1) = self.get_notebar_info(noteid.to_string(), ebid.to_string()) {
            let (cx, cy) = note_pos_to_chunkpos(n1.x, n1.y);
            let mut ck = self.get_note_chunk_ids(GetChunkNoteIdsReq {
                noteid: noteid.to_string(),
                chunkx: cx,
                chunky: cy,
            });
            //1.chunk 移除 bar
            ck.noteids.retain(|v| v.as_str().unwrap() != ebid);
            self.set_note_chunk_ids(noteid, cx, cy, &ck);
            //2.相连bar 移除
            for v in &n1.connected {
                let mut sp = v.as_str().unwrap().split("_");
                let mut a = sp.next().unwrap().to_string();
                let mut b = sp.next().unwrap().to_string();
                if b == ebid {
                    std::mem::swap(&mut a, &mut b);
                }
                let mut notebar = self
                    .get_notebar_info(noteid.to_string(), b.clone())
                    .unwrap();
                notebar
                    .connected
                    .retain(|v1| v1.as_str().unwrap() != v.as_str().unwrap());
                self.set_notebar_info(noteid, &*b, &notebar);
                self.remove_note_path_info(noteid, v.as_str().unwrap());
            }

            if self.fitdown_chunk_range_in_notemeta_if_ck_is_on_edge(noteid, cx, cy, &mut meta) {
                self.set_note_meta(noteid, &meta);
            }
        } else {
            eprintln!("error, delete bar ebid not found")
        }

        meta
    }
    pub fn add_path(&self, noteid: &str, ebid1: &str, ebid2: &str) -> Option<(i32, i32)> {
        if let (Some(mut n1), Some(mut n2)) = (
            self.get_notebar_info(noteid.to_string(), ebid1.to_string()),
            self.get_notebar_info(noteid.to_string(), ebid2.to_string()),
        ) {
            // todo
            //  检查是否已经有path
            n1.connected
                .push(Value::String(format!("{}_{}", ebid1, ebid2)));
            n2.connected
                .push(Value::String(format!("{}_{}", ebid1, ebid2)));
            n1.epoch_addup();
            n2.epoch_addup();
            self.set_notebar_info(noteid, ebid1, &n1);
            self.set_notebar_info(noteid, ebid2, &n2);
            let ret = Some((n1.epoch_i32(), n2.epoch_i32()));
            self.set_note_path_info(
                noteid,
                &*format!("{}_{}", ebid1, ebid2),
                &PathInfo { pathtype: 0 },
            );
            // let v1=Value::String()
            // n1.connected.binary_search(&)
            ret
        } else {
            eprintln!("error, add path ebid not found");
            None
        }
    }
    fn get_note_path_info(&self, noteid: &str, pathid: &str) -> Option<PathInfo> {
        let key = format!("{}|path|{}", noteid, pathid);
        let s = KvBatch::new().then_get(key.as_bytes()).finally_call();
        let s = s.res_str();
        match s {
            None => None,
            Some(s) => Some(serde_json::from_str(&*s).unwrap()),
        }
    }
    fn set_note_path_info(&self, noteid: &str, pathid: &str, info: &PathInfo) {
        let key = format!("{}|path|{}", noteid, pathid);
        // self.kernel.set(key, serde_json::to_string(info).unwrap());
        KvBatch::new()
            .then_set(
                key.as_bytes(),
                serde_json::to_string(info).unwrap().as_bytes(),
            )
            .finally_call();
    }
    pub fn remove_note_path_info(&self, noteid: &str, pathid: &str) {
        let key = format!("{}|path|{}", noteid, pathid);
        // self.kernel.del(key);
        KvBatch::new().then_delete(key.as_bytes()).finally_call();
    }

    fn get_article_list(&self, noteid: &str) -> ArticleList {
        let key = format!("{}|articlelist", noteid);
        match KvBatch::new()
            .then_get(key.as_bytes())
            .finally_call()
            .res_str()
        {
            // match self.kernel.get(key.clone()) {
            None => {
                // let store=ArticleList::default()
                println!("no article list stored");
                // self.kernel.set(key, "{\"list\":[]}".to_string());
                KvBatch::new()
                    .then_set(key.as_bytes(), "{\"list\":[]}".as_bytes())
                    .finally_call();
                ArticleList::default()
            }
            Some(serial) => {
                println!("get_article_list {}", serial);
                match serde_json::from_str::<ArticleList>(&*serial) {
                    Ok(v) => v,
                    Err(e) => {
                        // self.kernel.set(key, "{\"list\":[]}".to_string());
                        KvBatch::new()
                            .then_set(key.as_bytes(), "{\"list\":[]}".as_bytes())
                            .finally_call();
                        panic!("unserial article list failed {}", serial);
                    }
                }
            }
        }
    }
    fn set_article_list(&self, noteid: &str, article_list: &ArticleList) {
        let key = format!("{}|articlelist", noteid);
        // self.kernel
        //     .set(key.clone(), serde_json::to_string(article_list).unwrap());
        // println!("set article list {}", self.kernel.get(key).unwrap());
    }
    pub fn article_binder(&self, mut arg: ArticleBinderReq) -> ArticleBinderResp {
        let mut task_type = String::new();
        let mut noteid = String::new();
        std::mem::swap(&mut noteid, &mut arg.noteid);
        std::mem::swap(&mut task_type, &mut arg.bind_unbind_rename);
        let mut al = self.get_article_list(&*noteid);
        match &*task_type {
            "bind" => {
                let ret = ArticleBinderResp {
                    if_success: if al.bind(arg) { 1 } else { 0 },
                };
                self.set_article_list(&*noteid, &al);
                ret
            }
            "unbind" => {
                let ret = ArticleBinderResp {
                    if_success: if al.unbind(arg) { 1 } else { 0 },
                };
                self.set_article_list(&noteid, &al);
                ret
            }
            "rename" => {
                let ret = ArticleBinderResp {
                    if_success: if al.rename(arg) { 1 } else { 0 },
                };
                self.set_article_list(&noteid, &al);
                ret
            }
            _ => {
                eprintln!("unreachable task type {}", task_type);
                unreachable!()
            }
        }
    }
}

impl ContentApi for Impl {
    fn get_note_mata(&self, req: GetNoteMataReq) -> GetNoteMataResp {
        NoteMan::new().get_note_meta(&req.noteid)
    }
    fn get_chunk_note_ids(&self, req: GetChunkNoteIdsReq) -> GetChunkNoteIdsResp {
        NoteMan::new().get_note_chunk_ids(req)
    }
    fn get_note_bar_info(&self, req: GetNoteBarInfoReq) -> GetNoteBarInfoResp {
        let info = NoteMan::new().get_notebar_info(req.noteid, req.notebarid);
        if let Some(info) = info {
            info.to_reply()
        } else {
            GetNoteBarInfoResp {
                w: -1.0,
                h: -1.0,
                ..GetNoteBarInfoResp::default()
            }
        }
    }
    fn create_new_bar(&self, arg: CreateNewBarReq) -> CreateNewBarResp {
        let note_man = NoteMan::new();
        let (_created, (cx, cy)) = note_man.create_notebar(arg.x, arg.y, arg.noteid.clone());
        let ck_noteids = note_man.get_note_chunk_ids(GetChunkNoteIdsReq {
            noteid: arg.noteid,
            chunkx: cx,
            chunky: cy,
        });
        CreateNewBarResp {
            chunkx: cx,
            chunky: cy,
            noteids: ck_noteids.noteids,
        }
    }
    fn update_bar_content(&self, arg: UpdateBarContentReq) -> UpdateBarContentResp {
        let note_man = NoteMan::new();
        let mut nb = note_man
            .get_notebar_info(arg.noteid.clone(), arg.barid.clone())
            .unwrap();
        nb.formatted = arg.formatted;
        nb.text = arg.text;
        nb.update_edit_time();
        nb.epoch_addup();
        note_man.set_notebar_info(&*(arg.noteid), &*(arg.barid), &nb);

        UpdateBarContentResp {
            new_epoch: nb.epoch_i32(),
        }
    }
    fn update_bar_transform(&self, arg: UpdateBarTransformReq) -> UpdateBarTransformResp {
        let note_man = NoteMan::new();
        let mut nb = note_man
            .get_notebar_info(arg.noteid.clone(), arg.barid.clone())
            .unwrap();
        let (oldcx, oldcy) = note_pos_to_chunkpos(nb.x, nb.y);
        let (newcx, newcy) = note_pos_to_chunkpos(arg.x, arg.y);
        let (chunk_change, meta) = if oldcx != newcx || oldcy != newcy {
            let meta = note_man.change_notebar_chunk(
                &*(arg.noteid),
                &*(arg.barid),
                (oldcx, oldcy),
                (newcx, newcy),
            ); //变更区块下的信息
            (
                vec![
                    Value::String(chunkpos_to_chunkid(oldcx, oldcy)),
                    Value::String(chunkpos_to_chunkid(newcx, newcy)),
                ],
                meta,
            )
        } else {
            (vec![], note_man.get_note_meta(&*arg.noteid))
        };
        nb.x = arg.x;
        nb.y = arg.y;
        nb.w = arg.w;
        nb.h = arg.h;
        nb.epoch_addup();
        note_man.set_notebar_info(&*(arg.noteid), &*(arg.barid), &nb);
        UpdateBarTransformResp {
            new_epoch: nb.epoch_i32(),
            chunk_maxx: meta.max_chunkx,
            chunk_minx: meta.min_chunkx,
            chunk_maxy: meta.max_chunky,
            chunk_miny: meta.min_chunky,
            chunk_change,
        }
    }
    fn redo(&self, req: RedoReq) -> RedoResp {
        RedoResp::default()
    }
    fn add_path(&self, arg: AddPathReq) -> AddPathResp {
        let note_man = NoteMan::new();
        let res = note_man.add_path(&*(arg.noteid), &*(arg.from), &*(arg.to));

        if let Some((from_epoch, to_epoch)) = res {
            AddPathResp {
                _1succ_0fail: 1,
                new_epoch_from: from_epoch,
                new_epoch_to: to_epoch,
            }
        } else {
            AddPathResp {
                _1succ_0fail: 0,
                new_epoch_from: 0,
                new_epoch_to: 0,
            }
        }
    }
    fn get_path_info(&self, arg: GetPathInfoReq) -> GetPathInfoResp {
        let note_man = NoteMan::new();
        let info = note_man
            .get_note_path_info(&*(arg.noteid), &*(arg.pathid_with_line))
            .unwrap();
        GetPathInfoResp {
            type_: info.pathtype,
        }
    }
    fn set_path_info(&self, arg: SetPathInfoReq) -> SetPathInfoResp {
        let note_man = NoteMan::new();
        note_man.set_note_path_info(
            &*(arg.noteid),
            &*(arg.pathid_with_line),
            &PathInfo {
                pathtype: arg.type_,
            },
        );
        SetPathInfoResp::default()
    }
    fn remove_path(&self, arg: RemovePathReq) -> RemovePathResp {
        let note_man = NoteMan::new();
        // 对应editor bar 中的信息变更
        let mut split = arg.pathid_with_line.split("_");
        let a = split.next().unwrap().to_string();
        let b = split.next().unwrap().to_string();
        let mut ainfo = note_man
            .get_notebar_info(arg.noteid.clone(), a.clone())
            .unwrap();
        ainfo.remove_path(&*(arg.pathid_with_line));
        ainfo.epoch_addup();
        note_man.set_notebar_info(&*(arg.noteid), &*a, &ainfo);
        let mut binfo = note_man
            .get_notebar_info(arg.noteid.clone(), b.clone())
            .unwrap();
        binfo.remove_path(&*(arg.pathid_with_line));
        binfo.epoch_addup();
        note_man.set_notebar_info(&*(arg.noteid), &*b, &binfo);
        // 移除path
        note_man.remove_note_path_info(&*(arg.noteid), &*(arg.pathid_with_line));

        RemovePathResp {
            new_epoch_to: binfo.epoch.unwrap() as i32,
            new_epoch_from: ainfo.epoch.unwrap() as i32,
        }
    }
    fn delete_bar(&self, arg: DeleteBarReq) -> DeleteBarResp {
        let note_man = NoteMan::new();
        let meta = note_man.delete_bar(&*(arg.noteid), &*(arg.barid));
        DeleteBarResp {
            chunk_maxx: meta.max_chunkx,
            chunk_minx: meta.min_chunkx,
            chunk_maxy: meta.max_chunky,
            chunk_miny: meta.min_chunky,
        }
    }
    fn article_binder(&self, req: ArticleBinderReq) -> ArticleBinderResp {
        NoteMan::new().article_binder(req)
    }
    fn article_list(&self, mut arg: ArticleListReq) -> ArticleListResp {
        let note_man = NoteMan::new();

        let mut noteid = String::new();
        std::mem::swap(&mut noteid, &mut arg.noteid);
        let al = note_man.get_article_list(&*noteid);
        let mut collect_noteinfos:Vec<Value>//barid-time-article
            =Vec::new();
        for a in al.list {
            let bar = note_man.get_notebar_info(noteid.clone(), a.barid.clone());
            if let Some(mut bar_info) = bar {
                if let Some(time) = bar_info.edit_time {
                    let mut map = serde_json::Map::new();
                    map.insert("barid".to_string(), Value::from(a.barid));
                    map.insert("artname".to_string(), Value::from(a.artname));
                    map.insert("edittime".to_string(), Value::from(time));
                    collect_noteinfos.push(Value::Object(map));
                } else {
                    // eprintln!("articlelist collecting bar no edit time");
                    let time = time_stamp_ms_u64();
                    bar_info.edit_time = Some(time);
                    note_man.set_notebar_info(&*noteid, &*a.barid, &bar_info);

                    let mut map = serde_json::Map::new();
                    map.insert("barid".to_string(), Value::from(a.barid));
                    map.insert("artname".to_string(), Value::from(a.artname));
                    map.insert("edittime".to_string(), Value::from(time));
                    collect_noteinfos.push(Value::Object(map));
                }
            } else {
                eprintln!("articlelist collecting bar not found;")
            }
        }
        collect_noteinfos.sort_by(|v2, v1| {
            v1.as_object()
                .unwrap()
                .get("edittime")
                .unwrap()
                .as_u64()
                .unwrap()
                .cmp(
                    &v2.as_object()
                        .unwrap()
                        .get("edittime")
                        .unwrap()
                        .as_u64()
                        .unwrap(),
                )
        });

        println!("artile list collected {}", collect_noteinfos.len());

        ArticleListResp {
            if_success: 1,
            list: collect_noteinfos,
        }
    }
    fn fetch_all_note_bars_epoch(
        &self,
        arg: FetchAllNoteBarsEpochReq,
    ) -> FetchAllNoteBarsEpochResp {
        let note_man = NoteMan::new();

        let note_meta = note_man.get_note_meta(arg.noteid.as_str());
        let mut res = vec![];
        for x in note_meta.min_chunkx..note_meta.max_chunkx + 1 {
            for y in note_meta.min_chunky..note_meta.max_chunky + 1 {
                let ids = note_man.get_note_chunk_ids(GetChunkNoteIdsReq {
                    noteid: arg.noteid.clone(),
                    chunkx: x,
                    chunky: y,
                });
                fn get_chunk_note_ids_reply_to_noteids_strvec(
                    mut reply: GetChunkNoteIdsResp,
                ) -> Vec<String> {
                    let mut res = vec![];
                    while let Some(noteid) = reply.noteids.pop() {
                        res.push(noteid.as_str().unwrap().to_owned());
                    }
                    res
                }
                let mut notes = get_chunk_note_ids_reply_to_noteids_strvec(ids);
                while let Some(noteid) = notes.pop() {
                    let mut onepair = vec![];
                    onepair.reserve(2);
                    let info = note_man.get_notebar_info(arg.noteid.clone(), noteid.clone());
                    onepair.push(Value::String(noteid));
                    onepair.push(Value::Number(Number::from(info.unwrap().epoch.unwrap())));
                    res.push(Value::Array(onepair));
                }
            }
        }
        FetchAllNoteBarsEpochResp {
            bars_id_and_epoch: res,
        }
    }
}
