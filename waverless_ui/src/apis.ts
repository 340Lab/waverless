import axios from "axios"


export class NodeBasic {
    constructor(
        public name:string,
        public online:boolean,
        public ip:string,
        public ssh_port:string,
        public cpu_sum:number,
        public cpu_cur:number,
        public mem_sum:number,
        public mem_cur:number,
        public passwd:string,
        public system:string,
    ){}
}

export class Action {
    constructor(
        public name:string,
        public cmd:string,
    ){}
}

export class ServiceBasic {
    constructor(
        public name:string,
        public node:string,
        public dir:string,
        public actions:Action[],
    ){}
}


export class AddServiceRespSucc {
    constructor(

    ){}
}

export class AddServiceRespTemplate {
    constructor(
        public nodes:string[],
    ){}
}

export class AddServiceRespFail {
    constructor(
        public msg:string,
    ){}
}

export class AddServiceResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    succ():undefined| AddServiceRespSucc{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    template():undefined| AddServiceRespTemplate{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
    fail():undefined| AddServiceRespFail{
        if(this.id==3){
            return this.kernel
        }
        return undefined
    }
    
}


export class AddServiceReq {
    constructor(
        public service:ServiceBasic,
    ){}
}

export namespace apis {
    export async function add_service(req:AddServiceReq):Promise<AddServiceResp>{
        let res:any = await axios.post("/api/add_service", req)
        return new AddServiceResp(res.data.kernel,res.data.id)
    }
}




export class DeleteServiceRespSucc {
    constructor(

    ){}
}

export class DeleteServiceRespFail {
    constructor(
        public msg:string,
    ){}
}

export class DeleteServiceResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    succ():undefined| DeleteServiceRespSucc{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    fail():undefined| DeleteServiceRespFail{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


export class DeleteServiceReq {
    constructor(
        public service:string,
    ){}
}

export namespace apis {
    export async function delete_service(req:DeleteServiceReq):Promise<DeleteServiceResp>{
        let res:any = await axios.post("/api/delete_service", req)
        return new DeleteServiceResp(res.data.kernel,res.data.id)
    }
}




export class GetServiceListRespExist {
    constructor(
        public services:ServiceBasic[],
    ){}
}

export class GetServiceListResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    exist():undefined| GetServiceListRespExist{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
}


export namespace apis {
    export async function get_service_list():Promise<GetServiceListResp>{
        let res:any = await axios.post("/api/get_service_list", )
        return new GetServiceListResp(res.data.kernel,res.data.id)
    }
}




export class RunServiceActionRespSucc {
    constructor(
        public output:string,
    ){}
}

export class RunServiceActionRespFail {
    constructor(
        public msg:string,
    ){}
}

export class RunServiceActionResp{
    constructor(
        private kernel: any,
        private id: number
    ) {}
    
    succ():undefined| RunServiceActionRespSucc{
        if(this.id==1){
            return this.kernel
        }
        return undefined
    }
    
    fail():undefined| RunServiceActionRespFail{
        if(this.id==2){
            return this.kernel
        }
        return undefined
    }
    
}


export class RunServiceActionReq {
    constructor(
        public service:string,
        public action_cmd:string,
        public sync:boolean,
    ){}
}

export namespace apis {
    export async function run_service_action(req:RunServiceActionReq):Promise<RunServiceActionResp>{
        let res:any = await axios.post("/api/run_service_action", req)
        return new RunServiceActionResp(res.data.kernel,res.data.id)
    }
}


