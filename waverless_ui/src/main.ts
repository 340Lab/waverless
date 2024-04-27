import './assets/main.css'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'

import { createApp } from 'vue'
import App from './App.vue'

// import 'highlight.js/styles/stackoverflow-light.css'
import hljs from "highlight.js/lib/core";
import hljsVuePlugin from "@highlightjs/vue-plugin";

import bash from "highlight.js/lib/languages/bash";
hljs.registerLanguage("bash", bash);

const app = createApp(App)
app.use(ElementPlus)
app.use(hljsVuePlugin)
app.mount('#app')
