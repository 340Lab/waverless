<script lang="ts">
import Chart from "./Chart.vue";
import { request } from "../request";
import { ChartBuilder } from "../chart_builder";
export default {
  components: {
    Chart,
  },

  data() {
    return {
      _horizon_added: {},
      _builders: {
        builder_horizon: new ChartBuilder()
          .reset_horizon()
          .no_data_zoom()
          .name("横向比较"),
        builder_req: new ChartBuilder().reset().name("请求数"),
        builder_cpu: new ChartBuilder().reset().name("总cpu"),
        builder_mem: new ChartBuilder().reset().name("总内存"),
        builder_avg_req_done_time: new ChartBuilder()
          .reset()
          .name("平均请求完成时间"),
        builder_cost: new ChartBuilder().reset().name("总成本"),
        builder_cost_performance: new ChartBuilder().reset().name("性价比"),
      },
      _builder_charts: [],
    };
  },

  // `mounted` 是生命周期钩子，之后我们会讲到
  mounted() {
    // `this` 指向当前组件实例
    // let charts = [
    //   this.$refs.req_chart,
    //   this.$refs.cpu_chart,
    //   this.$refs.mem_chart,
    //   this.$refs.req_done_avg_chart,
    //   this.$refs.cost_chart,
    // ];
    this._builders.builder_horizon
      .reset_spec("cost_perform")
      .reset_spec("done_time")
      .reset_spec("cost");
    this._builder_charts = [
      [this._builders.builder_req, this.$refs.req_chart],
      [this._builders.builder_cpu, this.$refs.cpu_chart],
      [this._builders.builder_mem, this.$refs.mem_chart],
      [this._builders.builder_avg_req_done_time, this.$refs.req_done_avg_chart],
      [this._builders.builder_cost, this.$refs.cost_chart],
      [this._builders.builder_cost_performance, this.$refs.cost_performance],
    ];
    for (let i = 0; i < this._builder_charts.length; i++) {
      for (let j = i + 1; j < this._builder_charts.length; j++) {
        function a_dispatch_b(a, b) {
          a.on_zoom_cb.push(function (params) {
            // TODO - debounce
            const { start, end, startValue } = params;
            // console.log(start, end);
            // // zoom the others!
            if (!startValue) {
              b.dispatchAction({
                type: "dataZoom",
                // percentage of zoom start position, 0 - 100
                start: start,
                // percentage of zoom finish position, 0 - 100
                end: end,
                startValue: 1,
              });
            }
          });
        }
        let a = this._builder_charts[i][1];
        let b = this._builder_charts[j][1];
        a_dispatch_b(a, b.inner_chart());
        a_dispatch_b(b, a.inner_chart());
      }
    }
  },

  unmounted() {},

  methods: {
    frame_num(frame: any[]) {
      return frame[0];
    },
    frame_reqs(frame: any[]) {
      return frame[1];
    },
    frame_nodes(frame: any[]) {
      return frame[2];
    },
    frame_cpu(frame: any[]) {
      let sum = 0;
      let nodes = this.frame_nodes(frame);
      nodes.forEach((n) => {
        sum += n.c;
      });
      return sum;
    },
    frame_mem(frame: any[]) {
      let sum = 0;
      let nodes = this.frame_nodes(frame);
      nodes.forEach((n) => {
        sum += n.m;
      });
      return sum;
    },
    frame_avg_req_done_time(frame: any[]) {
      return frame[3];
    },
    clean_frame_draw(name: string) {
      console.log("clean_frame_draw");
      for (const key in this._builders) {
        this._builders[key].reset_spec(name);
      }
      this.apply_builder_result();
    },
    update_frames(name, frames: any[][]) {
      // this._name_frames[name] = frames;
      let {
        builder_req,
        builder_cpu,
        builder_mem,
        builder_avg_req_done_time,
        builder_cost,
        builder_cost_performance,
        builder_horizon,
      } = this._builders;

      for (const key in this._builders) {
        if (key != "builder_horizon") {
          this._builders[key].reset_spec(name);
        }
      }

      frames.forEach((frame) => {
        builder_req.add_col(
          name,
          this.frame_num(frame),
          this.frame_reqs(frame).length
        );
        builder_cpu.add_col(name, this.frame_num(frame), this.frame_cpu(frame));
        builder_mem.add_col(name, this.frame_num(frame), this.frame_mem(frame));
        builder_avg_req_done_time.add_col(
          name,
          this.frame_num(frame),
          this.frame_avg_req_done_time(frame)
        );
        builder_cost_performance.add_col(
          name,
          this.frame_num(frame),
          1 / this.frame_avg_req_done_time(frame) / frame[6]
        );
        builder_cost.add_col(name, this.frame_num(frame), frame[6]);
      });
      if (frames.length > 1995 && !(name in this._horizon_added)) {
        this._horizon_added[name] = 1;
        let frame = frames[1995];
        // this._name_rate[name] = {
        //   cost: frame[6],
        //   cost_perform: 1 / this.frame_avg_req_done_time(frame) / frame[6],
        //   req_time: this.frame_avg_req_done_time(frame),
        // };
        console.log("add cost", frame[6]);
        builder_horizon.add_col("cost", name, frame[6] * 8);
        builder_horizon.add_col(
          "cost_perform",
          name,
          (1 / this.frame_avg_req_done_time(frame) / frame[6]) * 50
        );
        builder_horizon.add_col(
          "done_time",
          name,
          this.frame_avg_req_done_time(frame)
        );
      }
      this.apply_builder_result();
    },
    apply_builder_result() {
      this._builder_charts.forEach((builder_chart) => {
        builder_chart[1].setOption(builder_chart[0].option);
      });
      // this.$refs.horizon.setOption(this._builders.builder_horizon.option);
    },
    chart_request_count() {},
  },
};
</script>

<template>
  <div class="history_metric">
    HistoryMetric
    <!-- <Chart class="chart2" ref="horizon" /> -->
    <Chart class="chart" ref="req_chart" />
    <Chart class="chart" ref="cpu_chart" />
    <Chart class="chart" ref="mem_chart" />
    <Chart class="chart" ref="cost_chart" />
    <Chart class="chart" ref="cost_performance" />
    <Chart class="chart" ref="req_done_avg_chart" />
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
.chart {
  height: 200px;
}
.chart2 {
  height: 400px;
  width: 400px;
  margin-left: 400px;
}
.history_metric {
  /* height: 100%; */
  overflow: scroll;
}
</style>
