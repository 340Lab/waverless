use crate::{
    kv::{data_router::DataRouter, dist_kv_raft::tikvraft_proxy::RaftModule},
    network::p2p::P2PModule,
    sys::LogicalModules,
};
use camelpaste::paste;

macro_rules! logical_modules_view_iter {
    ($pt:ty,$e:ident,$t:ty) => {
        paste! {
            impl [<$pt LMView>]{
                pub fn $e<'a>(&'a self) -> &'a $t {
                    unsafe { &*(*self.inner.get()).as_ref().unwrap().as_ptr() }.$e()
                }
            }
        }

        // {
        //     let val: usize = $e; // Force types to be integers
        //     println!("{} = {}", stringify!{$e}, val);
        // }
    };
    ($pt:ty,$e:ident,$t:ty, $($es:ident,$ts:ty),+) => {
        // impl LogicalModelsView{
        //     fn $e<'a'(&'a self) -> &'a LogicalModules {
        //         &self.$e
        //     }
        // }
        logical_modules_view_iter! { $e, $t }
        logical_modules_view_iter! { $($es,$ts),+ }
    };
}

macro_rules! logical_modules_view {
    (
        // $e:ident,$y:ty,
        $t:ty,$($es:ident,$ts:ty),+) => {
        paste! {
                // use std::cell::UnsafeCell;
            // use std::sync::Weak;
            pub struct [<$t LMView>]{
                inner: std::cell::UnsafeCell<
                    Option<
                        std::sync::Weak<LogicalModules>>>,
            }

            unsafe impl Send for [<$t LMView>]{}
            unsafe impl Sync for [<$t LMView>]{}

            impl [<$t LMView>]{
                #[must_use]
                pub fn new() -> Self{
                    Self{
                        inner: std::cell::UnsafeCell::new(None),
                    }
                }
            }
            impl $t{
                #[must_use]
                pub fn setup_logical_modules_view(&self, logical_modules: std::sync::Weak<LogicalModules>){
                    unsafe {
                        *self.logical_modules_view.inner.get() = Some(logical_modules);
                    }
                }
            }
            logical_modules_view_iter! { $t,$($es,$ts),+ }
        }


    };
}

// every module should be seen
logical_modules_view!(P2PModule, data_router, Option<DataRouter>);

logical_modules_view!(RaftModule, p2p, P2PModule);
