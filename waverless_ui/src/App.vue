<script lang="ts">
import CurrentState from "./components/CurrentState.vue";
import SideBar from "./components/SideBar.vue";
import HistoryMetric from "./components/HistoryMetric.vue";
import HorizontalComparation from "./components/HorizontalComparation.vue";
import SubPageServices from "./components/SubPageServices.vue";
import SubPageHeadServices from "./components/SubPageHeadServices.vue";
import DetailPanelService from "./components/DetailPanelService.vue";

import { request } from "./request";
import { local_cache } from "./local_cache";
import { page } from "@/page";

class FetchCanceller {
  cancel = false;
}

export default {
  components: {
    SideBar,
    CurrentState,
    HistoryMetric,
    HorizontalComparation,
    SubPageServices,
    SubPageHeadServices,
    DetailPanelService,
  },
  data() {
    return {};
  },

  // `mounted` 是生命周期钩子，之后我们会讲到
  mounted() {},
  methods: {},
  unmounted() {},
};
</script>

<template>
  <header>
    <!-- <img alt="Vue logo" class="logo" src="./assets/logo.svg" width="125" height="125" /> -->

    <!-- <div class="wrapper">
      <HelloWorld msg="You did it!" />
    </div> -->
  </header>

  <main>
    <div class="row_container">
      <SideBar :selected_keys="selected_keys" style="width: 200px" ref="sidebar" />
      <div style="height: 100%; overflow: scroll; width: 100%">
        <SubPageHeadServices></SubPageHeadServices>
        <SubPageServices></SubPageServices>
      </div>
      <DetailPanelService style="width: 1000px"></DetailPanelService>
      <!--      <CurrentState class="right_column" v-if="current_state_or_history" />-->
      <!--      <div class="right_column">-->
      <!--        HELLO-->
      <!--&lt;!&ndash;        <HorizontalComparation class="right" />&ndash;&gt;-->
      <!--&lt;!&ndash;        <HistoryMetric class="right" ref="history_metric" />&ndash;&gt;-->
      <!--      </div>-->
      <!-- <HistoryMetric /> -->
    </div>
  </main>
</template>

<style scoped>
.row_container {
  display: flex;
  flex-direction: row;
  height: calc(100vh - 20px);
}

.right {
}

.right_column {
  width: calc(100% - 200px);
  padding-left: 20px;
  flex-direction: column;
  height: 100%;
  overflow: scroll;
}

/* .col_item {
  width: 25%;
} */
</style>
