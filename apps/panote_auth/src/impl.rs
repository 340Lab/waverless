use crate::r#struct::*;
use crate::*;

use self::authority::AuthorityMan;

mod authority;

impl AuthApi for Impl {
    fn login(&self, req: LoginReq) -> LoginResp {
        let auth_man = AuthorityMan::new();
        if let Ok(_) = auth_man.check_id_and_pw(&*(req.id), &*(req.pw)) {
            LoginResp {
                if_success: 1,
                token: auth_man.gen_token(),
            }
        } else {
            LoginResp {
                if_success: 0,
                token: "".to_string(),
            }
        }
    }
    fn verify_token(&self, req: VerifyTokenReq) -> VerifyTokenResp {
        let auth_man = AuthorityMan::new();
        if auth_man.check_token(req.token).is_ok() {
            VerifyTokenResp {
                if_success: 1,
                new_token: auth_man.gen_token(),
            }
        } else {
            VerifyTokenResp {
                if_success: 0,
                new_token: "".to_string(),
            }
        }
    }
}
