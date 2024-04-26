### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


### read conf
import yaml
with open('http_conf.yaml') as f:
    yamldata = yaml.load(f, Loader=yaml.FullLoader)
BACKEND=yamldata["backend"]
FRONTEND=yamldata["frontend"]
STRUCTS=yamldata["structs"]
API_LIST=yamldata["api_list"]


### construct logics
import os

def big_camel(name):
    #snake to big camel
    return name.title().replace("_","")

def big_camel_2_snake(camel):
    #big camel to snake
    return "".join(["_"+c.lower() if c.isupper() else c for c in camel]).lstrip("_")

#######################################

def gen_type_ts(type):
    if isinstance(type,list):
        if type[0]=="Array":
            return f"{gen_type_ts(type[1])}[]"
        else:
            exit(f"unknown type {type}")
    else:
        if type=="String":
            return "string"
        elif type=="Int":
            return "number"
        elif type=="Float":
            return "number"
        elif type=="Bool":
            return "boolean"
        elif type=="Any":
            return "any"
        else:
            return type
            
def gen_type_rs(type):
    if isinstance(type,list):
        if type[0]=="Array":
            return f"Vec<{gen_type_rs(type[1])}>"
        else:
            exit(f"unknown type {type}")
    else:
        if type=="String":
            return "String"
        elif type=="Int":
            return "i32"
        elif type=="Float":
            return "f64"
        elif type=="Bool":
            return "bool"
        elif type=="Any":
            return "Value"
        else:
            return type

#######################################

def gen_struct_ts(struct_name,desc):
    content=""
    if desc!=None:
        for key, value in desc.items():
            content+=f"        public {key}:{gen_type_ts(value)},\n"
        content=content[:-1]
    return f"""
export class {struct_name} {{
    constructor(
{content}
    ){{}}
}}
"""

def gen_struct_body_rs(desc, with_pub):
    content=""
    if desc!=None:
        pub=""
        if with_pub:
            pub="pub "
        for key, value in desc.items():
            content+=f"       {pub}{key}:{gen_type_rs(value)},\n"
        content=content[:-1]
    return f"""{{
{content}
}}"""

def gen_struct_rs(struct_name,desc):
    return f"""
#[derive(Debug, Serialize, Deserialize)]
pub struct {struct_name} {gen_struct_body_rs(desc,True)}
"""

#######################################

def gen_dispatch_ts(name,desc):
    content=""

    # dispatch_types=["undefined"]
    dispatch_fns=[]
    idx=1
    for key, value in desc.items():
        content+=gen_struct_ts(name+key,value)
        dispatch_fns.append(f"""
    {big_camel_2_snake(key)}():undefined| {name+key}{{
        if(this.id=={idx}){{
            return this.kernel
        }}
        return undefined
    }}
    """)
        idx+=1
        

    content+=f"""
export class {name}{{
    constructor(
        private kernel: any,
        private id: number
    ) {{}}
    {"".join(dispatch_fns)}
}}
"""

    return content


def gen_dispatch_rs(name,desc):
    content=""

    # dispatch_types=["undefined"]
    dispatch_fns=[]

    dispatch_types=""
    dispatch_arms=""
    idx=1
    for key, desc in desc.items():
        # content+=gen_struct_rs(name+key,value)
        dispatch_types+=f"    {key}{gen_struct_body_rs(desc,False)},\n"
        dispatch_arms+=f"    {name}::{key}{{..}}=>{idx},\n"
        dispatch_fns.append(f"""
    fn {big_camel_2_snake(key)}(&self)->Option<&{name+key}> {{
        if self.id=={idx} {{
            return Some(&self.kernel)
        }}
        None
    }}
    """)
        idx+=1
        

    content+=f"""
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum {name}{{
{dispatch_types}
}}

impl {name} {{
    fn id(&self)->u32 {{
        match self {{
            {dispatch_arms}
        }}
    }}
    pub fn serialize(&self)->Value {{
        json!({{
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        }})
    }}
}}
"""
    return content
    
#######################################

def gen_front_ts():
    apis=f"{FRONTEND['header']}\n\n"

    for struct_name, desc in STRUCTS.items():
        apis+=gen_struct_ts(struct_name,desc)

    for api_name, api in API_LIST.items():
        reqtype=big_camel(api_name)+"Req"
        resptype=big_camel(api_name)+"Resp"

        req_struct=""
        fn_arg=""
        fn_arg2=""
        if api["req"]!=None:
            req_struct=gen_struct_ts(reqtype,api["req"])
            fn_arg=f"req:{reqtype}"
            fn_arg2="req"

        resp_content=gen_dispatch_ts(resptype,api["resp_dispatch"])
        
        apis+=f"""
{resp_content}
{req_struct}
export namespace apis {{
    export async function {api_name}({fn_arg}):Promise<{resptype}>{{
        let res:any = {FRONTEND["http_call"].format(api_name,fn_arg2)}
        return new {resptype}(res.data.kernel,res.data.id)
    }}
}}


"""
        
    # print(apis)
    os.makedirs(FRONTEND["dir"], exist_ok=True)
    with open(f'{FRONTEND["dir"]}/apis.ts', 'w') as f:
        f.write(apis)



def gen_back_rs():
    apis=f"""
use serde_json::{{json,Value}};
use serde::{{Serialize, Deserialize}};
use axum::{{http::StatusCode, routing::post, Json, Router}};
use async_trait::async_trait;
{BACKEND["header"]}
"""
    structs=[]
    for struct_name, desc in STRUCTS.items():
        structs.append(gen_struct_rs(struct_name,desc))
    apis+="".join(structs)

    handle_traits=[]
    api_registers=[]
    for api_name, api in API_LIST.items():
        reqtype=big_camel(api_name)+"Req"
        resptype=big_camel(api_name)+"Resp"

        req_struct=""
        fn_req_arg=""
        fn_req_arg2=""
        trait_arg=""
        if api["req"]!=None:
            req_struct=gen_struct_rs(reqtype,api["req"])
            fn_req_arg=f"Json(req):Json<{reqtype}>"
            fn_req_arg2="req"
            trait_arg=f"req:{reqtype}"

        resp_content=gen_dispatch_rs(resptype,api["resp_dispatch"])

        handle_traits.append(f"""
    async fn handle_{api_name}(&self, {trait_arg})->{resptype};
            """)

        api_registers.append(f"""
    async fn {api_name}({fn_req_arg})-> (StatusCode, Json<Value>){{
        (StatusCode::OK, Json(ApiHandlerImpl.handle_{api_name}({fn_req_arg2}).await.serialize()))
    }}
    router=router
        .route("/{api_name}", post({api_name}));
                             """)

    
        apis+=f"""
{resp_content}
{req_struct}
"""

    apis+=f"""
#[async_trait]
pub trait ApiHandler {{
    {"".join(handle_traits)}
}}


pub fn add_routers(mut router:Router)->Router
{{
    {"".join(api_registers)}
    
    router
}}

"""
    os.makedirs(BACKEND["dir"], exist_ok=True)
    with open(f'{BACKEND["dir"]}/apis.rs', 'w') as f:
        f.write(apis)

#######################################
    
def gen_front():
    if FRONTEND["lan"]=="ts":
        gen_front_ts()
    else:
        exit(f"unknown language {FRONTEND['lan']}")
def gen_back():
    if BACKEND["lan"]=="rs":
        gen_back_rs()
    else:
        exit(f"unknown language {BACKEND['lan']}")

#######################################

gen_front()
gen_back()
