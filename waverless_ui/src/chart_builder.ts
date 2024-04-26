export class ChartBuilder {
    option: any = {}
    reset() {
        // let base = +new Date(1988, 9, 3);
        // let oneDay = 24 * 3600 * 1000;
        // let data = [[base, Math.random() * 300]];

        this.option = {

            tooltip: {
                trigger: 'axis',
                // position: function (pt) {
                //     return [pt[0], '3%'];
                // }
            },
            title: {
                // left: 'center',
                text: '请求数统计'
            },
            toolbox: {
                feature: {
                    dataZoom: {
                        yAxisIndex: 'none'
                    },
                    restore: {},
                    saveAsImage: {}
                }
            },
            xAxis: {
                type: 'value',
                boundaryGap: false
            },
            yAxis: {
                type: 'value',
                boundaryGap: [0, '100%']
            },
            dataZoom: [
                {
                    type: 'inside',
                    start: 0,
                    end: 20
                },
                {
                    start: 0,
                    end: 20
                }
            ],
            series: [
                // {
                //     name: '处理中的请求数',
                //     type: 'line',
                //     smooth: true,
                //     symbol: 'none',
                //     areaStyle: {},
                //     data: []
                // }
            ]
        };

        return this
    }
    reset_horizon() {
        this.reset()
        this.option = {
            title: {
                // left: 'center',
                text: '请求数统计'
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
            xAxis:
            {
                type: "category",
                boundaryGap: false,
                data: [],
            },

            yAxis: [
                {
                    type: "value",
                },
            ],
            series: [
            ],
        }

        return this
    }
    no_data_zoom() {
        delete this.option["dataZoom"]
        return this
    }
    spec_indexs: any = {}
    reset_spec(spec_name: string) {
        if (!(spec_name in this.spec_indexs)) {
            // if (this.option.xAxis.type == "category") {
            //     this.option.xAxis.data.push(spec_name)
            // }
            this.option.series.push({
                name: spec_name,
                type: 'line',
                // smooth: true,
                // symbol: 'none',
                emphasis: {
                    focus: "series",
                },
                // areaStyle: {},
                data: []
            })

            this.spec_indexs[spec_name] = this.option.series.length - 1
        } else {
            this.option.series[this.spec_indexs[spec_name]].data = []
        }
        return this
    }
    name(name: string) {
        this.option.title.text = name
        return this
    }
    add_col(spec_name: string, x: any, y: number) {
        if (this.option.xAxis.type == "category") {

            let data = this.option.xAxis.data
            if (data[data.length - 1] != x) {
                this.option.xAxis.data.push(x)
            }
            this.option.series[this.spec_indexs[spec_name]].data.push(y)
            console.log("option", this.option)
            return this
        } else {
            this.option.series[this.spec_indexs[spec_name]].data.push([x, y])
            return this
        }

    }
    set_groups(type: "line" | "bar" | "boxplot", groups: {
        groupname: string,
        x_y_pairs: {
            x: number | string,
            y: number | number[],
        }[]
    }[]) {
        let title = this.option.title
        this.option = {
            legend: {
                top: "30px"
            },
            title,
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                },
                confine: true,
            },
            dataset: {
                dimensions: ['product'/*, '2015', '2016', '2017'*/],
                source: [
                    // { product: 'Matcha Latte', 2015: 43.3, 2016: 85.8, 2017: 93.7 },
                    // { product: 'Milk Tea', 2015: 83.1, 2016: 73.4, 2017: 55.1 },
                    // { product: 'Cheese Cocoa', 2015: 86.4, 2016: 65.2, 2017: 82.5 },
                    // { product: 'Walnut Brownie', 2015: 72.4, 2016: 53.9, 2017: 39.1 }
                ]
            },
            xAxis: { type: 'category' },
            yAxis: {},
            // Declare several bar series, each will be mapped
            // to a column of dataset.source by default.

            //{ type: 'bar' }, { type: 'bar' }, { type: 'bar' }
            series: []
        };

        if (groups.length > 0) {
            if (groups[0].x_y_pairs.length > 0) {
                for (const pair of groups[0].x_y_pairs) {
                    this.option.dataset.dimensions.push(pair.x.toString())
                    this.option.series.push({
                        type,
                        barGap: 0,
                        // label: {
                        //     show: true,
                        //     // position: app.config.position,
                        //     distance: -20,
                        //     // align: app.config.align,
                        //     align: 'right',
                        //     position: 'insideBottom',
                        //     // verticalAlign: 'middle',
                        //     rotate: 90,
                        //     formatter: '{a}',
                        //     // fontSize: 16,
                        //     // rich: {
                        //     //     name: {}
                        //     // }
                        // },
                    })
                }
                for (const group of groups) {
                    let source: { [key: string]: any } = {
                        product: group.groupname
                    }
                    for (const pair of group.x_y_pairs) {
                        source[pair.x.toString()] = pair.y
                    }
                    this.option.dataset.source.push(source)
                }
            }
        }
        return this
    }
    // remove(spec_name: string) {
    //     let i = this.spec_indexs[spec_name]
    //     let series = this.option.series
    //     this.option.series = series.slice(0, i - 1).concat(series.slice(i + 1, series.length))
    //     console.log("series after remove", this.option.series, series.slice(0, i - 1), series.slice(i + 1, series.length))
    //     if (this.option.series.length == 0) {
    //         this.option.series = [{
    //             name: spec_name,
    //             type: 'line',
    //             smooth: true,
    //             symbol: 'none',
    //             // areaStyle: {},
    //             data: []
    //         }]
    //     }
    //     return this
    // }
}