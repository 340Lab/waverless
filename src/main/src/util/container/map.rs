#[macro_export]
macro_rules! new_map {
    // 匹配空映射
    ($map_type:ident { }) => {
        $map_type::new()
    };
    // 匹配一个或多个键值对
    ($map_type:ident { $($key:expr => $value:expr),+ $(,)? }) => {{
        let map = $map_type::from([
            $( ($key, $value), )+
        ]);
        map
    }};
}
