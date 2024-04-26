<script lang="ts">
import { AddServiceReq, ServiceBasic, apis } from "@/apis";
import { h } from "vue";
import { ElNotification } from "element-plus";

export default {
  data() {
    return {
      dialogFormVisible: false,
      form: {
        name: "",
        node: "",
        dir: "",
        actions: [] as any[],
      },
      selectable_nodes: [] as string[],
      formLabelWidth: "90px",
      add_service_requesting: false,
      old_stringify: "",
    };
  },

  mounted() {
    this.start_add_service().then(() => {
      if (this.old_service) {
        this.form = {
          name: this.old_service.name,
          node: this.old_service.node,
          dir: this.old_service.dir,
          actions: this.old_service.actions.slice(),
        };
        this.old_stringify = JSON.stringify(this.form);
      }
    });
  },

  unmounted() {},

  methods: {
    // start_edit_service(service_basic) {
    //   this.start_add_service().then(() => {
    //     this.form = {
    //       name: service_basic.name,
    //       node: service_basic.node,
    //       dir: service_basic.dir,
    //       actions: service_basic.actions,
    //     };
    //   });
    // },
    start_add_service() {
      this.dialogFormVisible = true;
      return apis
        .add_service(new AddServiceReq(new ServiceBasic("", "", "", [])))
        .then((res) => {
          let temp = res.template();
          if (temp) {
            this.selectable_nodes = temp.nodes;
            console.log("selectable_nodes loaded", this.selectable_nodes);
          } else {
            console.warn("unexpect res", res);
          }
        })
        .catch((err) => {
          ElNotification({
            title: "获取服务配置失败",
            message: h("i", { style: "color: teal" }, "" + err),
          });
        });
    },
    confirm() {
      let action = "新增";
      if (this.old_service) {
        action = "修改";
        if (this.old_stringify == JSON.stringify(this.form)) {
          ElNotification({
            title: action + "服务配置信息失败",
            message: h("i", { style: "color: teal" }, "未修改"),
          });
          return;
        }
      }
      if (this.add_service_requesting) {
        return;
      }
      this.add_service_requesting = true;
      apis
        .add_service(
          new AddServiceReq(
            new ServiceBasic(
              this.form.name,
              this.form.node,
              this.form.dir,
              this.form.actions
            )
          )
        )
        .then((res) => {
          console.log(action + "service res", res);
          if (res.fail()) {
            ElNotification({
              title: action + "服务配置信息失败",
              message: h("i", { style: "color: teal" }, res.fail().msg),
            });
          } else if (res.succ()) {
            ElNotification({
              title: action + "服务成功",
              message: h("i", { style: "color: teal" }, res.succ().msg),
            });
            this.dialogFormVisible = false;
          }
        })
        .finally(() => {
          this.add_service_requesting = false;
        });
    },
    add_action() {
      this.form.actions.push({
        name: "",
        cmd: "",
      });
    },
    // init(_select_bar: (select: number, select_name: string) => void) {
    //   this._select_bar = _select_bar;
    // },
  },

  props: {
    old_service: {
      type: Object,
      default: undefined,
    },
  },
};
</script>

<template>
  <el-form :model="form">
    <el-form-item label="服务名" :label-width="formLabelWidth">
      <el-input v-model="form.name" autocomplete="off" />
    </el-form-item>
    <el-form-item label="所在节点" :label-width="formLabelWidth">
      <el-select v-model="form.node" placeholder="Please select a zone">
        <el-option
          v-for="item in selectable_nodes"
          :key="item"
          :label="item"
          :value="item"
        />
        <!-- <el-option label="lab1" value="docker" />
                    <el-option label="lab2" value="wasm" /> -->
      </el-select>
    </el-form-item>
    <el-form-item label="服务路径" :label-width="formLabelWidth">
      <el-input v-model="form.dir" autocomplete="off" />
    </el-form-item>
    <el-form-item label="Actions" :label-width="formLabelWidth">
      <el-row
        :gutter="10"
        v-for="(action, index) in form.actions"
        :key="index"
        style="padding-bottom: 10px"
      >
        <el-col :span="6">
          <el-input v-model="action.name" placeholder="name" />
        </el-col>
        <el-col :span="13">
          <el-input v-model="action.cmd" placeholder="cmd" />
        </el-col>
        <el-col :span="3">
          <el-button @click="form.actions.splice(index, 1)">删除</el-button>
        </el-col>
      </el-row>
      <el-button @click="add_action">增加action</el-button>
    </el-form-item>
    <!-- <el-form-item label="服务路径" :label-width="formLabelWidth">
                <el-select v-model="form.region" placeholder="Please select a zone">
                    <el-option label="Zone No.1" value="shanghai" />
                    <el-option label="Zone No.2" value="beijing" />
                </el-select>
            </el-form-item> -->
  </el-form>
</template>

<style scoped></style>
