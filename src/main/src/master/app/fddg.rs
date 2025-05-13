use crate::general::data::m_data_general::DATA_UID_PREFIX_FN_KV;
use crate::new_map;
use crate::util::container::sync_trie::SyncedTrie;
use crate::{
    general::app::{AppType, FnMeta},
    result::WSResult,
};
use std::collections::HashMap;

// function data dependency graph
// - need update when app uploaded
// - to find data binded functions
//   - co-scheduling data & functions
pub struct FDDGMgmt {
    // data_unique_id prefix -> app name -> (app_type, function names -> fn_meta)
    prefix_key_to_functions: SyncedTrie<HashMap<String, (AppType, HashMap<String, FnMeta>)>>,
}

// https://fvd360f8oos.feishu.cn/wiki/GGUnw0H1diVoHSkgm3vcMhtbnjI#share-QElHdn6dSoKVBUx5UssccxAZnnd
pub enum FuncTriggerType {
    DataWrite,
    DataNew,
    DataDelete,
}

impl FDDGMgmt {
    pub fn new() -> Self {
        Self {
            prefix_key_to_functions: SyncedTrie::new(),
        }
    }

    // return app_name -> (apptype, fn_name -> fn_meta)
    pub fn get_binded_funcs(
        &self,
        data_unique_id: &str,
        _ope: FuncTriggerType,
    ) -> HashMap<String, (AppType, HashMap<String, FnMeta>)> {
        let mut binded_funcs = HashMap::new();
        let binded_matchers = self.prefix_key_to_functions.match_partial(data_unique_id);
        for matcher in binded_matchers {
            tracing::debug!("match fddg data prefix len {:?}", matcher.0);
            let node = matcher.1.read();
            for (app_name, (app_type, fn_names)) in node.iter() {
                for (fn_name, fn_meta) in fn_names.iter() {
                    let _ = binded_funcs
                        .entry(app_name.to_string())
                        .and_modify(|(_, fn_names): &mut (AppType, HashMap<String, FnMeta>)| {
                            let _ = fn_names.insert(fn_name.to_string(), fn_meta.clone());
                        })
                        .or_insert_with(|| {
                            (
                                app_type.clone(),
                                new_map! (HashMap {
                                    fn_name.to_string() => fn_meta.clone(),
                                }),
                            )
                        });
                }
            }
            // let _ = binded_funcs
            //     .entry(app_name.to_string())
            //     .and_modify(|(_, fn_names)| {

            //     })
            //     .or_insert((*app_type, HashMap::new()));
        }
        tracing::debug!("binded_funcs get: {:?}", binded_funcs);
        binded_funcs
    }

    pub fn add_fn_trigger(
        &self,
        (app_name, app_type): (&str, AppType),
        (fn_name, fn_meta): (&str, &FnMeta),
    ) -> WSResult<()> {
        if let Some(data_accesses) = fn_meta.data_accesses.as_ref() {
            for (key_pattern, data_access) in data_accesses {
                let Some(_event) = data_access.event.as_ref() else {
                    continue;
                };

                let mut _hold = None;
                let insert_key: &str = if app_type == AppType::Native {
                    // without uid prefix
                    &*key_pattern.0
                } else {
                    // with uid prefix
                    _hold = Some(format!("{}{}", DATA_UID_PREFIX_FN_KV, key_pattern.0));
                    &*_hold.as_ref().unwrap()
                };

                let node = self
                    .prefix_key_to_functions
                    .search_or_insert(&insert_key, || {
                        new_map! (HashMap {
                            app_name.to_string() => {
                                (app_type, new_map! (HashMap {
                                    fn_name.to_string() => fn_meta.clone(),
                                }))
                            }
                        })
                    });
                let mut node = node.write();
                let _ = node
                    .entry(app_name.to_string())
                    .and_modify(|(_app_type, fn_names)| {
                        let _ = fn_names.insert(fn_name.to_string(), fn_meta.clone());
                    })
                    .or_insert_with(|| panic!("app_name not found, should be created when search"));
            }
        }
        Ok(())
    }
}
