<script lang="ts">
import { AddServiceReq, ServiceBasic, apis } from "@/apis";
import { share_var } from "@/share_var";

export default {
  data() {
    return {
      services: ["balaal"],
      share: share_var(),
    };
  },

  mounted() {
    this.update_services();
  },

  unmounted() {},

  methods: {
    update_services() {
      apis.get_service_list().then((res) => {
        console.log("get_service_list", res);
        if (res.exist()) {
          this.services = res.exist().services;
        }
      });
    },
    service_click(i) {
      this.share.service_basic_showing = this.services[i];

      console.log("service_click", this.share.select_service);
    },
    // init(_select_bar: (select: number, select_name: string) => void) {
    //   this._select_bar = _select_bar;
    // },
  },

  props: {
    // selected_keys: {
    //   type: Object,
    //   default: {},
    // },
  },
};
</script>

<template>
  <el-space wrap style="margin: 10px">
    <el-card
      v-for="(item, i) in services"
      @click="service_click(i)"
      :key="i"
      class="box-card"
      style="width: 250px"
    >
      <template #header>
        <div class="card-header">
          <span>{{ item.name }}</span>
          <!-- <el-button class="button" text>Operation button</el-button> -->
        </div>
      </template>
      <el-col>
        <el-row class="info_line">
          节点 <el-button link type="primary"> {{ item.node }} </el-button>
        </el-row>
        <el-row class="info_line">
          目录 <el-button link type="primary"> {{ item.dir }} </el-button>
        </el-row>
        <!-- <el-divider
          v-if="item.actions && item.actions.length > 0"
          content-position="left"
        ></el-divider>
        <el-row v-if="item.actions && item.actions.length > 0"> Actions </el-row> -->
      </el-col>
      <!-- <div>节点: {{ item.node }}</div>
      <div>目录: {{ item.dir }}</div>
      <div>Actions: {{ item.actions }}</div> -->
      <!-- <div v-for="o in 4" :key="o" class="text item">
                {{ 'List item ' + o }}
            </div> -->
    </el-card>
  </el-space>
</template>

<style scoped>
.info_line {
  margin: 0 0 10px 0;
}
</style>
