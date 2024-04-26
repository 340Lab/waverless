<script lang="ts">
import AddServiceDialogue from "./AddServiceDialogue.vue";

export default {
  data() {
    return {};
  },
  mounted() {},
  unmounted() {},
  methods: {
    // init(_select_bar: (select: number, select_name: string) => void) {
    //   this._select_bar = _select_bar;
    // },
    start_add_service() {
      this.$refs.add_service_dialogue.start_add_service();
    },
  },
  props: {
    // selected_keys: {
    //   type: Object,
    //   default: {},
    // },
  },
  components: { AddServiceDialogue },
};
</script>

<template>
  <div style="height: 60px; border-bottom: 1px solid var(--el-border-color); width: 100%">
    <el-button plain class="head_btn">刷新</el-button>
    <el-button plain class="head_btn" @click="start_add_service">增加服务</el-button>
    <AddServiceDialogue ref="add_service_dialogue"></AddServiceDialogue>
  </div>
</template>

<style scoped>
.head_btn {
  margin: 15px 0 15px 15px;
}
</style>
