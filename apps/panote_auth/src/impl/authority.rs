use self::token::CheckTokenRes;
use serde::{Deserialize, Serialize};
use wasm_serverless_lib::{HostFile, KvBatch, KvResult};

#[derive(Serialize, Deserialize, Default)]
pub struct AuthorityConfig {
    pub id: String,
    pub pw: String,
    pub token_secret: String,
}

mod token {
    use chrono;
    use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
    use serde::{Deserialize, Serialize};

    use super::AuthorityMan;

    // Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        // connid:i32,
        exp: u64,
    }

    // lazy_static! {
    //     pub static ref TOKEN_SECRET: RwLock<String> =RwLock::new( String::new());
    //     // pub static ref TEST:Test=Test::new();
    // }
    const EXPIRE_SEC: u64 = 60 * 60 * 24 * 15; //15 day

    //需要保证uid有效
    pub fn maketoken(auth_man: &AuthorityMan) -> String {
        let secret = &auth_man.0.token_secret;
        // 用于设置过期时间
        let second = chrono::Local::now().timestamp();

        // 生成JWT，主要用于保存uid
        // 用于加密的key是服务器配置的
        // todo : 安全性思考
        encode(
            &Header::default(),
            &Claims {
                exp: second as u64 + EXPIRE_SEC,
            },
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    pub enum CheckTokenRes {
        FailParse,
        Valid,
        Expire,
    }

    // 解析token
    pub fn checktoken(token: String, secret: &str) -> CheckTokenRes {
        // 解析用于验证的token
        let token = decode::<Claims>(
            &token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &Validation::default(),
        );
        match token {
            Ok(data) => {
                let second = chrono::Local::now().timestamp() as u64;

                println!(
                    "{} {}",
                    serde_json::to_string(&data.claims).unwrap(),
                    second
                );
                // 过期
                if second > data.claims.exp {
                    return CheckTokenRes::Expire;
                }
                CheckTokenRes::Valid
                // Ch
            }
            Err(e) => {
                // log::debug!("{}", e);
                print!("error:{}", e);

                CheckTokenRes::FailParse
            }
        }
    }
}

const CONFIG_FILE_PATH: &'static str = "authority_config.json";

pub struct AuthorityMan(AuthorityConfig);
impl AuthorityMan {
    pub fn new() -> Self {
        // let res = KvBatch::new()
        //     .then_get("authority_config".as_bytes())
        //     .finally_call();
        // let res = if let KvResult::Get(Some(res)) = &res[0] {
        //     serde_json::from_slice(&res).unwrap()
        // } else
        {
            let mut vec = Vec::with_capacity(300);
            HostFile::open(CONFIG_FILE_PATH).read_at(0, &mut vec);
            let res = serde_json::from_slice(&vec).unwrap();
            // KvBatch::new()
            //     .then_set("authority_config".as_bytes(), &vec)
            //     .finally_call();

            Self(res)
        }
    }
    fn config(&self) -> &AuthorityConfig {
        &self.0
    }
    pub fn check_id_and_pw(&self, id: &str, pw: &str) -> Result<(), ()> {
        let config = self.config();
        if config.pw == pw && config.id == id {
            return Ok(());
        }
        Err(())
    }
    pub fn gen_token(&self) -> String {
        token::maketoken(self)
    }
    pub fn check_token(&self, token: String) -> Result<(), ()> {
        match token::checktoken(token, &self.0.token_secret) {
            CheckTokenRes::Valid => Result::Ok(()),
            _ => Result::Err(()),
        }
    }
}
