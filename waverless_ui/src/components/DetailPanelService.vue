<script lang="ts">
import EditService from "@/components/EditService.vue";
import { AddServiceReq, ServiceBasic, apis, RunServiceActionReq } from "@/apis";
import { share_var } from "@/share_var";
import { h } from "vue";
import { ElNotification } from "element-plus";

export default {
  data() {
    return {
      share_var: share_var(),
      editing_service: false,
      formLabelWidth: "90px",
      requesting_actions: {},
      actions_records: {},
    };
  },

  mounted() {},

  unmounted() {},

  watch: {
    "share_var.service_basic_showing": {
      handler: function (val, oldVal) {
        this.reset_data();
        // console.log("watch", val, oldVal);
      },
      deep: false,
    },
  },

  methods: {
    reset_data() {
      this.requesting_actions = {};
      this.editing_service = false;
    },
    select_service(service_basic) {},
    // init(_select_bar: (select: number, select_name: string) => void) {
    //   this._select_bar = _select_bar;
    // },
    start_edit_service() {
      this.editing_service = true;
    },
    confirm_edit() {
      this.$refs.edit_service.confirm();
    },

    add_action_record(action, res) {
      if (!(action in this.actions_records)) {
        this.actions_records[action] = [];
      }
      this.actions_records[action].push(res);

      // let tid = action + (this.actions_records[action].length - 1);
      // setTimeout(() => {
      //   console.log(tid, document.getElementById(tid));
      //   var term = new Terminal();
      //   // const fitAddon = new FitAddon();
      //   // term.loadAddon(fitAddon);
      //   // term.onResize((size) => {
      //   //   console.log("onResize", size.cols, size.rows);
      //   //   term.resize(size.cols, size.rows);
      //   // });

      //   term.open(document.getElementById(tid));
      //   term.write(res);

      //   // fitAddon.fit();
      // }, 1000);
    },
    run_action(service: string, action: string) {
      console.log("run_action", service, action);
      if (action in this.requesting_actions) {
        ElNotification({
          title: "正在请求",
          message: h("i", { style: "color: teal" }, "请稍后再试"),
        });
        return;
      }
      this.requesting_actions[action] = 1;
      apis
        .run_service_action(new RunServiceActionReq(service, action, true))
        .then((res) => {
          console.log("run_service_action", res);
          if (res.fail()) {
            ElNotification({
              title: "请求失败",
              message: h("i", { style: "color: red" }, res.fail().msg),
            });
          } else if (res.succ()) {
            ElNotification({
              title: "请求成功",
              message: h("i", { style: "color: teal" }, res.succ().output),
            });
            this.add_action_record(action, res.succ().output);
          }
        })
        .finally(() => {
          delete this.requesting_actions[action];
        });
    },
  },

  props: {
    // selected_keys: {
    //   type: Object,
    //   default: {},
    // },
  },
  components: {
    EditService,
  },
};
</script>

<template>
  <div class="body">
    <div v-if="editing_service">
      <EditService ref="edit_service" :old_service="share_var.service_basic_showing" />
      <el-row justify="end">
        <el-button @click="editing_service = false">取消</el-button>
        <el-button @click="confirm_edit()"> 提交</el-button>
      </el-row>
    </div>
    <div v-else-if="share_var.service_basic_showing">
      <el-form>
        <el-form-item label="服务名" :label-width="formLabelWidth">
          {{ share_var.service_basic_showing.name }}
        </el-form-item>
        <el-form-item label="所在节点" :label-width="formLabelWidth">
          <el-button link type="primary">
            {{ share_var.service_basic_showing.node }}
          </el-button>
        </el-form-item>
        <el-form-item label="服务路径" :label-width="formLabelWidth">
          <el-button link type="primary">
            {{ share_var.service_basic_showing.dir }}
          </el-button>
        </el-form-item>
        <el-form-item label="Actions" :label-width="formLabelWidth">
          <div
            v-for="(action, index) in share_var.service_basic_showing.actions"
            :key="index"
            style="padding: 10px 0 10px 0; width: 100%"
          >
            <el-row justify="space-between">
              <el-popover
                placement="top-start"
                :width="350"
                trigger="hover"
                :content="action.cmd"
              >
                <template #reference>
                  <el-button
                    @click="run_action(share_var.service_basic_showing.name, action.cmd)"
                  >
                    {{ action.name }}
                  </el-button>
                </template>
              </el-popover>
            </el-row>
            <el-row
              v-if="action.cmd in actions_records"
              v-for="(record, index) in actions_records[action.cmd]"
              :key="index"
            >
              <div style="width: 500px; height: 200px; overflow: scroll">
                <highlightjs language="bash" :code="record"></highlightjs>
              </div>
              <!-- <el-input
                style="padding: 10px 10px 0 0px"
                :value="record"
                :rows="2"
                type="textarea"
                placeholder="Please input"
              /> -->
            </el-row>
          </div>
        </el-form-item>
      </el-form>
      <el-row justify="end">
        <el-button @click="start_edit_service()">编辑</el-button>
      </el-row>
    </div>
    <div v-else>请选择左侧服务列表查看详情 {{ share_var }}</div>
  </div>
</template>

<style scoped>
.info_line {
  margin: 0 0 10px 0;
}
.body {
  border-left: 1px solid var(--el-border-color);
  padding: 15px;
}
</style>
