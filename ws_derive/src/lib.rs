#![allow(clippy::all)]
#![deny(
    // unused_imports,
    unused_variables,
    // unused_mut,
    clippy::unnecessary_mut_passed,
    unused_results
)]

use std::collections::HashMap;

use convert_case::{Case, Casing};
use proc_macro::{self, TokenStream};
use quote::{quote, ToTokens};
use syn::{
    braced, bracketed, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, token, DeriveInput, Ident, Result, Token,
};

fn path_is_option(path: &syn::Path) -> bool {
    path.segments.len() == 1 && path.segments[0].ident == "Option"
}

// // HelloMacro 定义
// #[proc_macro_derive(LogicalModuleParent, attributes(sub, parent))]
// pub fn logical_module_parent_macro_derive(input: TokenStream) -> TokenStream {
//     let input: DeriveInput = parse_macro_input!(input);
//     // ident 当前枚举名称
//     let DeriveInput { ident, .. } = input;

//     let mut iter_arms = Vec::new();

//     if let syn::Data::Struct(data_struct) = input.data {
//         let mut match_index = -1;
//         for f in &data_struct.fields {
//             let mut is_sub = false;
//             let mut is_parent = false;

//             // let mut subtype = 0;
//             if f.attrs
//                 .iter()
//                 .find(|v| v.path.segments[0].ident.to_string() == "sub")
//                 .is_some()
//             {
//                 is_sub = true;
//             } else if f
//                 .attrs
//                 .iter()
//                 .find(|v| v.path.segments[0].ident.to_string() == "parent")
//                 .is_some()
//             {
//                 is_parent = true;
//             }

//             if is_sub || is_parent {
//                 match_index += 1;
//                 let match_index = match_index as usize;
//                 if let Type::Path(tp) = f.ty.clone() {
//                     let attrname = f.ident.clone().unwrap();
//                     if path_is_option(&tp.path) {
//                         let no_sub_iter_case = if is_sub {
//                             quote! {
//                                 if let Some(sub) = &unsafe { self.inner_ref.as_ref() }.#attrname{

//                                     self.index+=1;
//                                     self.current_sub_iter=None;
//                                     return Some(sub);
//                                 }else
//                                 {
//                                     self.index+=1;
//                                     self.current_sub_iter=None;
//                                     continue;
//                                 }
//                             }
//                         } else {
//                             quote!(
//                                 if let Some(sub) = &unsafe { self.inner_ref.as_ref() }.#attrname{
//                                     self.current_sub_iter = Some(Box::new(sub.module_iter()));
//                                     if let Some(sub_sub) = self.current_sub_iter.as_mut().unwrap().next_module(){
//                                         return Some(sub_sub);
//                                     }else{
//                                         self.index+=1;
//                                         return Some(sub)
//                                     }
//                                 }else
//                                 {
//                                     self.index+=1;
//                                     self.current_sub_iter=None;
//                                     continue;
//                                 }
//                             )
//                         };

//                         iter_arms.push(quote! {
//                         #match_index =>{
//                             // last time bind
//                             if let Some(sub_iter)= &mut self.current_sub_iter{
//                                 // tracing::info!("last time bind, index:{}",self.index);
//                                 // old sub iter
//                                 if let Some(sub) = sub_iter.next_module(){
//                                     return Some(sub);
//                                 }else{
//                                     self.index+=1;
//                                     self.current_sub_iter=None;
//                                     return Some(unsafe { self.inner_ref.as_ref() }.#attrname.as_ref().unwrap());
//                                 }
//                             }else{
//                             // new field
//                                 // tracing::info!("new field, index:{}",self.index);
//                                 #no_sub_iter_case
//                             }
//                         },
//                     });
//                         continue;
//                     } else if tp.path.segments.len() == 1 {
//                         // panic!("debug:{:?}", f);

//                         // if iter_arms.len() == 1 {
//                         //     panic!("{} {}", match_index, attrname)
//                         // }
//                         let no_sub_iter_case = if is_sub {
//                             quote! {
//                                 self.index+=1;
//                                 self.current_sub_iter=None;
//                                 return Some(sub);
//                             }
//                         } else {
//                             quote!(
//                                 self.current_sub_iter = Some(Box::new(sub.module_iter()));
//                                 if let Some(sub_sub) = self.current_sub_iter.as_mut().unwrap().next_module(){
//                                     return Some(sub_sub);
//                                 }else{
//                                     self.index+=1;
//                                     self.current_sub_iter=None;
//                                     return Some(sub)
//                                 }
//                             )
//                         };

//                         iter_arms.push(quote! {
//                             #match_index =>{
//                                 let sub=&unsafe { self.inner_ref.as_ref() }.#attrname;
//                                 if let Some(sub_iter)= &mut self.current_sub_iter{
//                                     // tracing::info!("last time bind, index:{}",self.index);
//                                     // old sub iter
//                                     if let Some(sub) = sub_iter.next_module(){
//                                         return Some(sub);
//                                     }else{
//                                         self.index+=1;
//                                         self.current_sub_iter=None;
//                                         return Some(sub);
//                                     }
//                                 }else{
//                                     #no_sub_iter_case
//                                 }
//                                 // self.index+=1;
//                                 // return Some(&unsafe { self.inner_ref.as_ref() }.#attrname);
//                             },
//                         });

//                         // iter_arms.pop();
//                         continue;
//                     }
//                 }
//             }
//         }
//     }

//     // let iter_name = format_ident!("Iterator{}", ident);

//     // 实现 comment 方法
//     let output = quote! {

//         impl<'a> ModuleIterTrait<'a> for ModuleIter<'a,#ident>{
//             fn next_module(& mut self) ->  Option<&'a dyn LogicalModule> {
//                 // tracing::info!("index:{:?}",self.index);
//                 loop{
//                     match self.index {
//                         #(#iter_arms)*
//                         _=>{
//                             // tracing::info!("unmatch index:{:?}",index);
//                             return None;
//                         }
//                     }
//                 }

//             }
//         }

//         impl<'a> LogicalModuleParent<'a> for #ident{
//             fn module_iter(&'a self) -> ModuleIter<'a,Self> {
//                 ModuleIter{
//                     inner_ref:self.into(),
//                     index:0,
//                     current_sub_iter:None,
//                     phantom: std::marker::PhantomData,
//                 }
//             }
//         }

//     };
//     output.into()
// }

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

            pub fn name() -> &'static str {
                stringify!(#ident)
            }
        }

    };
    output.into()
}

// Parse an outer attribute like:
//
//     #[repr(C, packed)]

#[derive(Debug)]
struct AttributeParen {
    // paren_token: token::Paren,
    // content: proc_macro2::TokenStream,
    idents: Vec<Ident>,
}

impl Parse for AttributeParen {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut content = vec![];
        loop {
            if let Ok(res) = input.parse::<Ident>() {
                content.push(res);
            } else if let Ok(_res) = input.parse::<Token![,]>() {
            } else {
                break;
            }
        }

        Ok(AttributeParen {
            // paren_token: parenthesized!(content in input),
            idents: content,
        })
    }
}

#[derive(Debug)]
struct OuterAttribute {
    // pound_token: Token![#],
    // bracket_token: token::Bracket,
    // ident: Ident,
    // group: token::Group,
    // paren: AttributeParen,
    content: TokenStream,
}

impl Parse for OuterAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut content;
        let _pound_token: Token![#] = input.parse()?;
        let _bracket_token: token::Bracket = bracketed!(content in input);
        let _ident: Ident = content.parse()?;
        let group: proc_macro2::Group = content.parse()?;
        let stream: TokenStream = group.stream().into();
        // let paren: AttributeParen = parse_macro_input!(stream);
        Ok(OuterAttribute {
            // pound_token,
            // bracket_token,
            // ident,
            // paren,
            content: stream,
        })
    }
}

#[proc_macro_derive(ModuleView, attributes(view))]
pub fn module_view_macro_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    // ident 当前枚举名称
    let DeriveInput { ident, .. } = input;

    let mut quotes = vec![];

    if let syn::Data::Struct(data_struct) = input.data {
        // 1. build a map field name -> field type
        let mut map = HashMap::new();
        for f in &data_struct.fields {
            let _ = map.insert(f.ident.clone().unwrap(), f.ty.clone());
        }

        for f in data_struct.fields {
            let mut one_module_views = vec![];
            for attr in f.attrs {
                for seg in &attr.path.segments {
                    if seg.ident == "view" {
                        let attr_stream: TokenStream = attr.to_token_stream().into();
                        // let views_stream: TokenStream = attr.tokens.clone().into();
                        let parsed: OuterAttribute = parse_macro_input!(attr_stream);
                        let inner_views = parsed.content.clone();
                        let parsed: AttributeParen = parse_macro_input!(inner_views);
                        for i in parsed.idents {
                            let i_type = map.get(&i).unwrap_or_else(|| {
                                panic!("{} not found in map", i);
                            });
                            let i_str = i.to_string();
                            let panic =
                                format!("INNER MODULES PTR NOT SETTED for {} view!!!", i_str);
                            one_module_views.push(quote! {
                                pub fn #i(&self)->&#i_type{
                                    //unsafe return
                                    unsafe{
                                        &(*self.inner.as_ref().expect(#panic).as_ptr()).#i
                                    }
                                    // &self.inner.upgrade().unwrap().#i
                                }
                            });
                        }

                        // panic!("{:?}", one_module_views[0].to_string());
                    }
                }
            }
            let mut field_type = f.ident.unwrap().to_string().to_case(Case::UpperCamel);
            field_type = format!("{}View", field_type);
            let fieldty_ident = Ident::new(&field_type, proc_macro2::Span::call_site());
            quotes.push(quote! {
                pub struct #fieldty_ident{
                    inner:Option<Weak<#ident>>,
                }

                impl #fieldty_ident{
                    pub fn new()->Self{
                        Self{
                            inner:None,
                        }
                    }

                    #(#one_module_views)*
                }
            });
            // panic!("{:?}", quotes[0].to_string());
        }
    }
    // 实现 comment 方法
    let output = quote! {


        #(#quotes)*
            // pub fn new(args: LogicalModuleNewArgs) -> Self {
            //     let ret = Self::inner_new(args);
            //     // tracing::info!("new module {}", ret.name());
            //     ret
            // }

            // pub fn name() -> &'static str {
            //     stringify!(#ident)
            // }


    };
    // panic!("{:?}", output.to_string());
    output.into()
}

// #[proc_macro_derive(AutoGenView, attributes(view))]
// pub fn auto_gen_view_macro_derive(input: TokenStream) -> TokenStream {
//     let input: DeriveInput = parse_macro_input!(input);
//     // ident 当前枚举名称
//     let DeriveInput { ident, .. } = input;

//     let mut comment_arms = Vec::new();

//     if let syn::Data::Enum(syn::DataEnum { variants, .. }) = input.data {
//         for variant in variants {
//             // 当前枚举项名称如 Alex, Box
//             let _ident_item = &variant.ident;

//             panic!("{:?}", variant.attrs);
//             // variant.attrs.iter().find(|attr|attr.pound_token)

//             // 根据属性值转为 OicColumn 定义的结构化数据
//             // if let Ok(column) = OicColumn::from_attributes(&variant.attrs) {
//             //     // 获取属性中的comment信息
//             //     let msg: &syn::Lit = &column.comment.unwrap();

//             //     // 生成 match 匹配项 Robot::Alex => "msg"
//             //     comment_arms.push(quote! ( #ident::#ident_item => #msg ));
//             // } else {
//             //     comment_arms.push(quote! ( #ident::#ident_item => "" ));
//             // }
//         }
//     }
//     if comment_arms.is_empty() {
//         comment_arms.push(quote! ( _ => "" ));
//     }

//     // // 实现 comment 方法
//     let output = quote! {

//         impl #ident {
//         //     fn comment(&self) -> &'static str {
//         //         match self {
//         //             #(#comment_arms),*
//         //         }
//         //     }
//         }
//     };

//     output.into()
// }
