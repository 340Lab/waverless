<script lang="ts">
import { request } from "@/request";
export default {
  data() {
    return {
      count: 1,
      timeout: 0,
      state: {
        node2node_graph: [],
      },
      score: 0,
    };
  },

  // `mounted` 是生命周期钩子，之后我们会讲到
  mounted() {
    // `this` 指向当前组件实例
    console.log(this.count); // => 1

    // 数据属性也可以被更改
    this.count = 2;
    this.timeout = setInterval(() => {
      request
        .state_score()
        .request()
        .then((res) => {
          this.state = res.data.state;
          this.score = res.data.score;
        });
    }, 300);
  },

  unmounted() {
    clearInterval(this.timeout);
  },
};
</script>

<template>
  <div class="current_state">
    <!-- <TheWelcome /> -->
    <!-- hello {{ state.requests }} -->
    <div>
      Current frame: {{ state.cur_frame }} Score: {{ score }} Cost:
      {{ state.cost }} 请求完成时间平均:
      {{ state.req_done_time_avg }} 请求完成时间方差:
      {{ state.req_done_time_std }} 90% 请求完成时间平均:
      {{ state.req_done_time_avg_90p }}
    </div>
    <div class="row">
      <div class="col">
        <h1>Handling Requests</h1>
        <div v-for="(value, key) in state.requests" :key="key">
          req_id: {{ value.req_id }}, app_id: {{ value.dag_id }}, done_fns:
          {{ value.done_fns }}, total_fn_cnt: {{ value.total_fn_cnt }},
          working_fns:{{ value.working_fns }}
        </div>
      </div>
      <div class="col">
        <h1>Done Requests</h1>
        <div v-for="(value, key) in state.done_requests" :key="key">
          req_id: {{ value.req_id }}, app_id: {{ value.dag_id }}, begin:
          {{ value.start_frame }}, end: {{ value.end_frame }}
        </div>
      </div>
      <div class="col">
        <h1>Nodes</h1>
        <div v-for="(value, key) in state.nodes" :key="key">
          mem: {{ value.used_mem }}/{{ value.mem }}, cpu:
          {{ value.used_cpu }}/{{ value.cpu }}, running:
          {{ value.running_req_fns.length }}
          <!-- <div
            style="padding-left: 20px"
            v-for="(value, key) in value.running_req_fns"
            :key="key"
          >
            {{ value }}
          </div> -->
        </div>
      </div>
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
.current_state {
  height: 100%;
  overflow: scroll;
}
</style>
