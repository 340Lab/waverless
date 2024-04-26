export namespace util {

    export class Config {
        constructor(
            /// for the different algos, should use the same seed
            public rand_seed: string,
            //  low middle high
            public request_freq: string,
            //  dag type: single, chain, dag, mix
            public dag_type: string,
            //  cold start: high, low, mix
            public cold_start: string,
            //  cpu, data, mix
            public fn_type: string,
            //  each stage control algorithm settings
            public es: {
                /// "ai","lass","fnsche","hpa","faasflow"
                up: string,
                // "ai","lass","fnsche","hpa","faasflow"
                down: string,
                // "rule","fnsche","faasflow"
                sche: string,
                // ai_type  sac, ppo, mat
                ai_type: string | undefined,
                // direct smooth_30 smooth_100
                down_smooth: string,
            },
        ) { }

        // serial the config attrbute not in remove
        serial(remove: string[] = []): string {
            const serializedConfig: string[] = [];

            if (!remove.includes("sd")) {
                serializedConfig.push(`sd${this.rand_seed}`);
            }
            if (!remove.includes("rf")) {
                serializedConfig.push(`rf${this.request_freq}`);
            }
            if (!remove.includes("dt")) {
                serializedConfig.push(`dt${this.dag_type}`);
            }
            if (!remove.includes("cs")) {
                serializedConfig.push(`cs${this.cold_start}`);
            }
            if (!remove.includes("ft")) {
                serializedConfig.push(`ft${this.fn_type}`);
            }

            const es = this.es;

            if (!remove.includes("up")) {
                serializedConfig.push(`up${es.up}`);
            }
            if (!remove.includes("dn")) {
                serializedConfig.push(`dn${es.down}`);
            }
            if (!remove.includes("sc")) {
                serializedConfig.push(`sc${es.sche}`);
            }
            if (!remove.includes("at") && es.ai_type !== undefined) {
                serializedConfig.push(`at${es.ai_type}`);
            }
            if (!remove.includes("ds")) {
                serializedConfig.push(`ds${es.down_smooth}`);
            }


            return serializedConfig.join(".");
        }
    }
    export function parse_config_str(config_str: string): Config {

        let config: Config = new Config(
            "",
            "",
            "",
            "",
            "",
            {
                up: "",
                down: "",
                sche: "",
                ai_type: undefined,
                down_smooth: "",
            },
        );

        const configs: string[] = config_str.split(".");
        let idx: number = 0;


        outer:
        for (const config_part of configs) {
            switch (idx) {
                case 0:
                    config.rand_seed = config_part.replace("sd", "");
                    break;
                case 1:
                    config.request_freq = config_part.replace("rf", "");
                    break;
                case 2:
                    config.dag_type = config_part.replace("dt", "");
                    break;
                case 3:
                    config.cold_start = config_part.replace("cs", "");
                    break;
                case 4:
                    config.fn_type = config_part.replace("ft", "");
                    break;
                case 5:
                    config.es.up = config_part.replace("up", "");
                    break;
                case 6:
                    config.es.down = config_part.replace("dn", "");
                    break;
                case 7:
                    config.es.sche = config_part.replace("sc", "");
                    break;
                case 8:
                    if (config_part.includes("at")) {
                        config.es.ai_type = config_part.replace("at", "");
                        break;
                    }
                case 9:
                    config.es.down_smooth = config_part.replace("ds", "");
                    break outer;
                default:
                    throw new Error("impossible");
            }
            idx++;
        }

        return config
    }
}