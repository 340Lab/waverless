<template>
  <v-chart class="chart" ref="chart" :option="option" @datazoom="on_zoom" />
</template>

<script>
import { use } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { LineChart, BarChart } from "echarts/charts";

import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
} from "echarts/components";
import VChart, { THEME_KEY } from "vue-echarts";
import * as echarts from "echarts";
// import "echarts/map/js/china.js"
// use([
//     CanvasRenderer,
//     TitleComponent,
//     TooltipComponent,
//     LegendComponent,
//     LineChart
// ]);

export default {
  name: "line_chart3",
  components: {
    VChart,
  },
  provide: {
    [THEME_KEY]: "light",
  },
  data() {
    return {
      on_zoom_cb: [],
      option: {
        title: {
          text: "Stacked Area Chart",
        },
        // backgroundColor: "transparent",
        tooltip: {
          trigger: "axis",
          axisPointer: {
            type: "cross",
            label: {
              backgroundColor: "#6a7985",
            },
          },
        },
        legend: {
          data: ["Email", "Union Ads", "Video Ads", "Direct", "Search Engine"],
        },
        toolbox: {
          feature: {
            saveAsImage: {},
          },
        },
        grid: {
          left: "3%",
          right: "4%",
          bottom: "3%",
          containLabel: true,
        },
        xAxis: [
          {
            type: "category",
            boundaryGap: false,
            data: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
          },
        ],
        yAxis: [
          {
            type: "value",
          },
        ],
        series: [
          {
            name: "Email",
            type: "line",
            stack: "Total",
            areaStyle: {},
            emphasis: {
              focus: "series",
            },
            data: [120, 132, 101, 134, 90, 230, 0],
          },
          {
            name: "Union Ads",
            type: "line",
            stack: "Total",
            areaStyle: {},
            emphasis: {
              focus: "series",
            },
            data: [220, 182, 191, 234, 290, 330, 310],
          },
          {
            name: "Video Ads",
            type: "line",
            stack: "Total",
            areaStyle: {},
            emphasis: {
              focus: "series",
            },
            data: [150, 232, 201, 154, 190, 330, 410],
          },
          {
            name: "Direct",
            type: "line",
            stack: "Total",
            areaStyle: {},
            emphasis: {
              focus: "series",
            },
            data: [320, 332, 301, 334, 390, 330, 320],
          },
          {
            name: "Search Engine",
            type: "line",
            stack: "Total",
            label: {
              show: true,
              position: "top",
            },
            areaStyle: {},
            emphasis: {
              focus: "series",
            },
            data: [820, 932, 901, 934, 1290, 1330, 0],
          },
        ],
      },
    };
  },
  props: {
    // tablename: {
    //     type: String,
    //     default: ""
    // }
  },
  mounted() {
    // this.setChart();
  },
  beforeDestroy() {},
  methods: {
    setOption(opt) {
      this.option = opt;
    },
    on_zoom(param) {
      this.on_zoom_cb.forEach((f) => {
        f(param);
      });
    },
    inner_chart() {
      return this.$refs.chart;
    },
  },
};
</script>


<style scoped>
.chart {
  /* border: 1px solid red; */
  /* background-color: red; */
  /* width: 300px; */
  /* height: 120px; */
  height: 100%;
  width: 100%;
  z-index: 1000;
}
</style>
