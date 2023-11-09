#![allow(clippy::all)]
#![deny(
    unused_imports,
    unused_variables,
    unused_mut,
    clippy::unnecessary_mut_passed,
    unused_results
)]

use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Type};

fn path_is_option(path: &syn::Path) -> bool {
    path.segments.len() == 1 && path.segments[0].ident == "Option"
}

// HelloMacro 定义
#[proc_macro_derive(LogicalModuleParent, attributes(sub, parent))]
pub fn logical_module_parent_macro_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    // ident 当前枚举名称
    let DeriveInput { ident, .. } = input;

    let mut iter_arms = Vec::new();

    if let syn::Data::Struct(data_struct) = input.data {
        let mut match_index = -1;
        for f in &data_struct.fields {
            let mut is_sub = false;
            let mut is_parent = false;

            // let mut subtype = 0;
            if f.attrs
                .iter()
                .find(|v| v.path.segments[0].ident.to_string() == "sub")
                .is_some()
            {
                is_sub = true;
            } else if f
                .attrs
                .iter()
                .find(|v| v.path.segments[0].ident.to_string() == "parent")
                .is_some()
            {
                is_parent = true;
            }

            if is_sub || is_parent {
                match_index += 1;
                let match_index = match_index as usize;
                if let Type::Path(tp) = f.ty.clone() {
                    let attrname = f.ident.clone().unwrap();
                    if path_is_option(&tp.path) {
                        let no_sub_iter_case = if is_sub {
                            quote! {
                                if let Some(sub) = &unsafe { self.inner_ref.as_ref() }.#attrname{

                                    self.index+=1;
                                    self.current_sub_iter=None;
                                    return Some(sub);
                                }else
                                {
                                    self.index+=1;
                                    self.current_sub_iter=None;
                                    continue;
                                }
                            }
                        } else {
                            quote!(
                                if let Some(sub) = &unsafe { self.inner_ref.as_ref() }.#attrname{
                                    self.current_sub_iter = Some(Box::new(sub.module_iter()));
                                    if let Some(sub_sub) = self.current_sub_iter.as_mut().unwrap().next_module(){
                                        return Some(sub_sub);
                                    }else{
                                        self.index+=1;
                                        return Some(sub)
                                    }
                                }else
                                {
                                    self.index+=1;
                                    self.current_sub_iter=None;
                                    continue;
                                }
                            )
                        };

                        iter_arms.push(quote! {
                        #match_index =>{
                            // last time bind
                            if let Some(sub_iter)= &mut self.current_sub_iter{
                                // tracing::info!("last time bind, index:{}",self.index);
                                // old sub iter
                                if let Some(sub) = sub_iter.next_module(){
                                    return Some(sub);
                                }else{
                                    self.index+=1;
                                    self.current_sub_iter=None;
                                    return Some(unsafe { self.inner_ref.as_ref() }.#attrname.as_ref().unwrap());
                                }
                            }else{
                            // new field
                                // tracing::info!("new field, index:{}",self.index);
                                #no_sub_iter_case
                            }
                        },
                    });
                        continue;
                    } else if tp.path.segments.len() == 1 {
                        // panic!("debug:{:?}", f);

                        // if iter_arms.len() == 1 {
                        //     panic!("{} {}", match_index, attrname)
                        // }
                        let no_sub_iter_case = if is_sub {
                            quote! {
                                self.index+=1;
                                self.current_sub_iter=None;
                                return Some(sub);
                            }
                        } else {
                            quote!(
                                self.current_sub_iter = Some(Box::new(sub.module_iter()));
                                if let Some(sub_sub) = self.current_sub_iter.as_mut().unwrap().next_module(){
                                    return Some(sub_sub);
                                }else{
                                    self.index+=1;
                                    self.current_sub_iter=None;
                                    return Some(sub)
                                }
                            )
                        };

                        iter_arms.push(quote! {
                            #match_index =>{
                                let sub=&unsafe { self.inner_ref.as_ref() }.#attrname;
                                if let Some(sub_iter)= &mut self.current_sub_iter{
                                    // tracing::info!("last time bind, index:{}",self.index);
                                    // old sub iter
                                    if let Some(sub) = sub_iter.next_module(){
                                        return Some(sub);
                                    }else{
                                        self.index+=1;
                                        self.current_sub_iter=None;
                                        return Some(sub);
                                    }
                                }else{
                                    #no_sub_iter_case
                                }
                                // self.index+=1;
                                // return Some(&unsafe { self.inner_ref.as_ref() }.#attrname);
                            },
                        });

                        // iter_arms.pop();
                        continue;
                    }
                }
            }
        }
    }

    // let iter_name = format_ident!("Iterator{}", ident);

    // 实现 comment 方法
    let output = quote! {



        impl<'a> ModuleIterTrait<'a> for ModuleIter<'a,#ident>{
            fn next_module(& mut self) ->  Option<&'a dyn LogicalModule> {
                // tracing::info!("index:{:?}",self.index);
                loop{
                    match self.index {
                        #(#iter_arms)*
                        _=>{
                            // tracing::info!("unmatch index:{:?}",index);
                            return None;
                        }
                    }
                }

            }
        }

        impl<'a> LogicalModuleParent<'a> for #ident{
            fn module_iter(&'a self) -> ModuleIter<'a,Self> {
                ModuleIter{
                    inner_ref:self.into(),
                    index:0,
                    current_sub_iter:None,
                    phantom: std::marker::PhantomData,
                }
            }
        }

    };
    output.into()
}

#[proc_macro_derive(LogicalModule)]
pub fn logical_module_macro_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    // ident 当前枚举名称
    let DeriveInput { ident, .. } = input;

    // 实现 comment 方法
    let output = quote! {

        impl #ident{
            pub fn new(args: LogicalModuleNewArgs) -> Self {
                let ret = Self::inner_new(args);
                // tracing::info!("new module {}", ret.name());
                ret
            }

            pub fn self_name() -> &'static str {
                stringify!(#ident)
            }
        }

    };
    output.into()
}
