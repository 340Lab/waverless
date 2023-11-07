use std::ptr::NonNull;

use crate::sys::{LogicalModule, LogicalModules};
pub use ws_derive::{LogicalModule, LogicalModuleParent};

pub struct ModuleIter<'a, P: LogicalModuleParent<'a>> {
    // fields maybe optional
    pub inner_ref: NonNull<P>,
    pub index: usize,
    pub current_sub_iter: Option<Box<dyn ModuleIterTrait<'a> + 'a>>,
    pub phantom: std::marker::PhantomData<&'a P>,
}

impl<'a, P: LogicalModuleParent<'a>> ModuleIter<'a, P> {
    pub fn inner_ref(&self) -> &P {
        unsafe { self.inner_ref.as_ref() }
    }
}

pub trait ModuleIterTrait<'a> {
    fn next_module(&mut self) -> Option<&'a dyn LogicalModule>;
}

pub trait LogicalModuleParent<'a>: Sized {
    fn module_iter(&'a self) -> ModuleIter<'a, Self>;
}

// impl<'a> ModuleIterTrait<'a> for ModuleIter<'a, LogicalModules> {
//     fn next_module(&mut self) -> Option<&'a dyn LogicalModule> {
//         let index = self.index;
//         loop {
//             match index {
//                 0usize => {
//                     self.index += 1;
//                     return Some(&unsafe { self.inner_ref.as_ref() }.kv_client);
//                 }
//                 1usize => {
//                     self.index += 1;
//                     return Some(&unsafe { self.inner_ref.as_ref() }.data_router_client);
//                 }
//                 2usize => {
//                     self.index += 1;
//                     return Some(&unsafe { self.inner_ref.as_ref() }.p2p_client);
//                 }
//                 3usize => {
//                     self.index += 1;
//                     return Some(&unsafe { self.inner_ref.as_ref() }.p2p);
//                 }
//                 4usize => {
//                     if let Some(sub_iter) = &mut self.current_sub_iter {
//                         if let Some(sub) = sub_iter.next_module() {
//                             return Some(sub);
//                         } else {
//                             self.index += 1;
//                             continue;
//                         }
//                     } else {
//                         if let Some(sub) = &unsafe { self.inner_ref.as_ref() }.data_router {
//                             self.current_sub_iter = Some(Box::new(sub.module_iter()));
//                             if let Some(sub_sub) =
//                                 self.current_sub_iter.as_mut().unwrap().next_module()
//                             {
//                                 return Some(sub_sub);
//                             } else {
//                                 self.index += 1;
//                                 return Some(sub);
//                             }
//                         } else {
//                             self.index += 1;
//                             continue;
//                         }
//                     }
//                 }
//                 _ => {
//                     return None;
//                 }
//             }
//         }
//     }
// }
// impl<'a> LogicalModuleParent<'a> for LogicalModules {
//     fn module_iter(&'a self) -> ModuleIter<'a, Self> {
//         ModuleIter {
//             inner_ref: self.into(),
//             index: 0,
//             current_sub_iter: None,
//             phantom: std::marker::PhantomData,
//         }
//     }
// }
