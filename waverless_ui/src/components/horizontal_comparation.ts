import { ChartBuilder } from "@/chart_builder";
import { util } from "@/util";

export namespace horizontal_comparation {
    export class DataCollector {

        name = "demo"
        filter = (config_str: string): boolean => {
            let config = util.parse_config_str(config_str);
            return (
                config.dag_type == "single" &&
                config.cold_start == "high" &&
                config.fn_type == "cpu"
            );
        }
        // return group_name - config_strs
        group = (config_strs: string[]): any[] => {
            let map: any = {};
            for (const config_str of config_strs) {
                let config = util.parse_config_str(config_str);
                let group_name = config.request_freq;
                if (!map[group_name]) {
                    map[group_name] = [];
                }
                map[group_name].push(config_str);
            }
            let res = [[], [], []]
            res[0] = map["low"]
            res[1] = map["middle"]
            res[2] = map["high"]
            return res;
        }
        group_by_abridge = "rf"
        metric: "cost_perform" | "cost" | "time" = "cost_perform"
    }
    export const builder_chart_datacollector = [
        [
            new ChartBuilder().reset().name("demo"),
            undefined,
            new DataCollector(),
        ],
    ]
} 