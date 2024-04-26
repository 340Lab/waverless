import axios from "axios"

const SIMSERVER_ADDR = "/api/"

export namespace request {
    export function state_score() {
        let req = new Request()
        req.api = "state_score"
        return req
    }
    export function history_list() {
        let req = new Request()
        req.api = "history_list"
        return req
    }
    export function history(name: string, begin: number, end: number) {

        let req = new Request()
        req.api = "history"
        req.data = { name, begin, end }
        console.log("history new", name, begin, end, req.data)
        return req
    }

    /// [seed->[[configstr, cost, time, score],...]]
    export async function get_seeds_metrics(seeds: string[]): Promise<{ [key: string]: any[][] }> {
        let req = new Request()
        req.api = "get_seeds_metrics"
        req.data = seeds
        let res = await req.request()
        return res.data
    }

    export class Request {
        api = ""
        data: any = undefined

        async request() {
            return request(this.api, this.data)
        }
    }

    async function request(api: string, data: any) {
        let res = undefined
        if (data) {
            // console.log("request data", data)
            res = await axios.post(SIMSERVER_ADDR + api, data)
        } else {

            res = await axios.post(SIMSERVER_ADDR + api)
        }
        // console.log("req res", res)
        return res
    }
}

