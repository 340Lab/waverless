#[macro_export]
macro_rules! with_option {
    // // 匹配简单变量
    // ($opt:ident, $var:ident => $body:block) => {{
    //     let mut __temp = $opt.take().expect("Option is None");
    //     let $var = &mut __temp;
    //     let __result = (|| $body)();
    //     $opt = Some(__temp);
    //     __result
    // }};

    // 匹配表达式
    ($opt:expr, $var:ident => $body:block) => {{
        // let mut __source = &mut $opt;
        if let Some($var) = $opt.take() {
            let _ = $opt.replace($body);
        }
    }};

    // 匹配类型注解情况
    (($opt:expr): $type:ty, $var:ident => $body:block) => {{
        if let Some($var) = $opt.take() {
            let _ = $opt.replace($body);
        }
    }};
}
