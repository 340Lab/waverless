<script lang="ts">
import Chart from "./Chart.vue";
import { request } from "../request";
import { ChartBuilder } from "../chart_builder";
import { util } from "../util";
import { horizontal_comparation } from "./horizontal_comparation";

export default {
  components: {
    Chart,
  },

  data() {
    return {
      _builder_chart_datacollectors: [],
    };
  },

  // `mounted` 是生命周期钩子，之后我们会讲到
  mounted() {
    this.prepare_buidler_and_charts();
    this.fetch_data();
  },

  unmounted() {},

  methods: {
    prepare_buidler_and_charts() {
      this._builder_chart_datacollectors = [];
      // `this` 指向当前组件实例
      let group = (config_strs: string[]): any => {
        let map: any = {};
        for (const config_str of config_strs) {
          let config = util.parse_config_str(config_str);
          let group_name = config.request_freq;
          if (!map[group_name]) {
            map[group_name] = [];
          }
          map[group_name].push(config_str);
        }
        let res = [[], [], []];
        res[0] = ["low", map["low"]];
        res[2] = ["high", map["high"]];
        res[1] = ["middle", map["middle"]];
        return res;
      };
      let filter = (config_str): boolean => {
        let config = util.parse_config_str(config_str);
        return (
          config.dag_type == "single" &&
          config.cold_start == "high" &&
          config.fn_type == "cpu" &&
          ["rule", "fnsche", "faasflow"].includes(config.es.sche)
        );
      };
      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_req_freq_cost_perform,
        {
          name: "不同请求负载下的性价比",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost_perform",
          group_by_abridge: "rf",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_req_freq_cost,
        {
          name: "不同请求负载下的成本",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost",
          group_by_abridge: "rf",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_req_freq_time,
        {
          name: "不同请求负载下的请求平均延迟",
          filter,
          // return group_name - config_strs
          group,
          metric: "time",
          group_by_abridge: "rf",
        },
      ]);

      // 复杂DAG应用
      group = (config_strs: string[]): any => {
        let map: any = {};
        for (const config_str of config_strs) {
          let config = util.parse_config_str(config_str);
          let group_name = config.request_freq;
          if (!map[group_name]) {
            map[group_name] = [];
          }
          map[group_name].push(config_str);
        }
        let res = [];
        res.push(["low", map["low"]]);
        res.push(["middle", map["middle"]]);
        res.push(["high", map["high"]]);
        // res[1] = ["single", map["single"]];
        // res[2] = ["high", map["high"]];
        return res;
      };
      filter = (config_str): boolean => {
        let config = util.parse_config_str(config_str);
        return (
          // config.request_freq == "middle" &&
          config.dag_type == "dag" &&
          config.cold_start == "high" &&
          config.fn_type == "cpu" &&
          ["rule", "fnsche", "faasflow"].includes(config.es.sche)
        );
      };
      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_dag_type_cost_perform,
        {
          name: "复杂DAG应用下的性价比",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost_perform",
          group_by_abridge: "dt",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_dag_type_cost,
        {
          name: "复杂DAG应用下的成本",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost",
          group_by_abridge: "rf",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_dag_type_time,
        {
          name: "复杂DAG应用下的请求平均延迟",
          filter,
          // return group_name - config_strs
          group,
          metric: "time",
          // group_by_abridge: "rf",
        },
      ]);

      // 复杂DAG应用
      group = (config_strs: string[]): any => {
        let map: any = {};
        for (const config_str of config_strs) {
          let config = util.parse_config_str(config_str);
          let group_name = config.fn_type + "_" + config.dag_type;
          if (!map[group_name]) {
            map[group_name] = [];
          }
          map[group_name].push(config_str);
        }
        let res = [];
        res.push(["dag_cpu", map["cpu_dag"]]);
        // res.push(["cpu_low", map["cpu_low"]]);
        res.push(["dag_data", map["data_dag"]]);
        res.push(["single", map["cpu_single"]]);
        // res.push(["data_low", map["data_low"]]);

        // res[0] = ["single", map["single"]];
        // res[1] = ["single", map["single"]];
        // res[2] = ["high", map["high"]];
        return res;
      };
      filter = (config_str): boolean => {
        let config = util.parse_config_str(config_str);
        return (
          config.request_freq == "middle" &&
          // config.dag_type == "dag" &&
          config.cold_start == "high" &&
          // config.fn_type == "cpu" &&
          config.es.up == "hpa"
        );
      };

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_sche_cost_perform,
        {
          name: "不同调度算法下的性价比",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost_perform",
          group_by_abridge: "dt",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_sche_cost,
        {
          name: "不同调度算法下的成本",
          filter,
          // return group_name - config_strs
          group,
          metric: "cost",
          group_by_abridge: "rf",
        },
      ]);

      this._builder_chart_datacollectors.push([
        new ChartBuilder().reset(),
        this.$refs.diff_sche_time,
        {
          name: "不同调度算法下的请求平均延迟",
          filter,
          // return group_name - config_strs
          group,
          metric: "time",
          group_by_abridge: "rf",
        },
      ]);
    },

    async fetch_data() {
      // console.log("horizontal comparation fetch_data");
      // [seed->[[configstr, cost, time, score],...]]
      let seeds_metrics = await request.get_seeds_metrics(["hello"]);
      console.log(
        "horizontal comparation seeds_metrics",
        seeds_metrics,
        this._builder_chart_datacollectors
      );

      // builder_chart_datacollector = this._builder_chart_datacollector;
      for (const builder_chart_datacollector of this
        ._builder_chart_datacollectors) {
        let builder: ChartBuilder = builder_chart_datacollector[0];
        let chart = builder_chart_datacollector[1];
        let datacollector: horizontal_comparation.DataCollector =
          builder_chart_datacollector[2];
        console.log("horizontal comparation1", datacollector);
        for (const seed in seeds_metrics) {
          // [[configstr, cost, time, score],...]]
          let seed_metrics: any[] = seeds_metrics[seed];
          let configstr2metrics: {
            [key: string]: { cost: number; time: number; score: number };
          } = {};
          console.log("horizontal comparation2");

          // filter the data we need in one chart
          let configstrs = seed_metrics
            .map((one_metric) => {
              let configstr: string = one_metric[0];
              configstr2metrics[configstr] = {
                cost: one_metric[1],
                time: one_metric[2],
                score: one_metric[3],
              };
              return configstr;
            })
            .filter((configstr) => {
              return datacollector.filter(configstr);
            });

          // group the data we need in one chart
          let groupname2configstrs = datacollector.group(configstrs);
          console.log(
            "horizontal comparation groupname2configstrs",
            groupname2configstrs
          );

          let groups = [];
          let record_x_orders_map = {};
          let order_x_map = {};
          let next_x_order = 0;
          let x_order = (x) => {
            if (x in record_x_orders_map) {
            } else {
              record_x_orders_map[x] = next_x_order;
              order_x_map[next_x_order] = x;
              next_x_order++;
            }

            return record_x_orders_map[x];
          };
          groupname2configstrs.forEach((groupname_configstrs) => {
            let [groupname, configstrs] = groupname_configstrs;
            let group = {
              groupname: groupname,
              x_y_pairs: [],
            };
            // let configstrs = groupname2configstrs[groupname];
            configstrs.forEach((configstr) => {
              let y = 0.0;
              if (datacollector.metric == "cost_perform") {
                let metric = configstr2metrics[configstr];
                let cost_multiply_time = metric.cost * metric.time;
                y = cost_multiply_time == 0 ? 0 : 1 / cost_multiply_time;
              } else {
                let metric = configstr2metrics[configstr];
                y = metric[datacollector.metric];
              }
              let x = util
                .parse_config_str(configstr)
                .serial(["sd", "rf", "dt", "cs", "ft", "dn", "ds"]);
              while (x_order(x) >= group.x_y_pairs.length) {
                group.x_y_pairs.push({ x: 0, y: 0 });
              }
              group.x_y_pairs[x_order(x)] = {
                x,
                y,
              };
            });
            groups.push(group);
          });

          // align groups x_y_pairs
          groups.forEach((group) => {
            while (group.x_y_pairs.length < next_x_order) {
              group.x_y_pairs.push({
                x: order_x_map[group.x_y_pairs.length],
                y: 0,
              });
            }
          });
          console.log("groups", groups);
          builder.name(datacollector.name).set_groups("bar", groups);
          chart.setOption(builder.option);
          console.log("horizontal_comparation");
        }
      }
    },
  },
};
</script>

<template>
  <div class="horizontal_comparation">
    <h1>HorizontalComparation</h1>

    <div class="chartsrow">
      <Chart class="chart" ref="diff_req_freq_cost_perform" />
      <Chart class="chart" ref="diff_req_freq_cost" />
      <Chart class="chart" ref="diff_req_freq_time" />
    </div>
    <div class="chartsrow">
      <Chart class="chart" ref="diff_dag_type_cost_perform" />
      <Chart class="chart" ref="diff_dag_type_cost" />
      <Chart class="chart" ref="diff_dag_type_time" />
    </div>
    <div class="chartsrow">
      <Chart class="chart" ref="diff_sche_cost_perform" />
      <Chart class="chart" ref="diff_sche_cost" />
      <Chart class="chart" ref="diff_sche_time" />
    </div>
  </div>
</template>

<style scoped>
.row {
  display: flex;
  flex-direction: row;
}
.col {
  width: 25%;
}
.chartsrow {
  display: flex;
  flex-direction: row;
}
.chart {
  height: 350px;
  flex: 1;
}
</style>
