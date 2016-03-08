/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//vis
function VisNetWork() {

}
VisNetWork.prototype = {
    newOptions: function (flags) {
        var fontFace = 'lato,roboto,"helvetica neue","segoe ui",arial,helvetica,sans-serif';
        var maxNodeRadius = 25;

        var verticalMargin = 28;
        var horizontalMargin = 50;
        var treeDistance = 100;
        var levelDistance = 120;
        var chartMinHeight = 250;
        var chartMinWidth = 400;
        return {
            autoResize: true, // The network will automatically detect size changes and redraw itself accordingly
            interaction: {
                hover: true,
                zoomView: false
            },
            //height: 100 + "%",
            height: Math.max(chartMinHeight,
                (treeDistance * flags.breadth + verticalMargin * 2)) + 'px',
            //width: 600+"px",
            width: Math.max(chartMinWidth,
                (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + horizontalMargin * 2)) + 'px',
            layout: {
                hierarchical: {
                    sortMethod: 'directed',
                    direction: 'LR',
                    levelSeparation: levelDistance
                }
            },
            nodes: {
                shape: 'dot',
                font: {
                    size: 13,
                    face: fontFace,
                    strokeColor: '#fff',
                    strokeWidth: 5
                },
                scaling: {
                    min: 10,
                    max: maxNodeRadius
                }
            },
            edges: {
                arrows: {
                    to: true
                },
                font: {
                    size: 11,
                    face: fontFace,
                    align: 'middle'
                },
                color: {
                    opacity: 1
                },
                smooth: true,
                scaling: {
                    min: 1,
                    max: 5
                }
            },
            physics: {
                solver: "repulsion",
                repulsion: {
                    centralGravity: 1
                }
            }
        };
    },

    newData: function (data) {
        var self = this;
        var nodes = [];
        var edges = [];
        var spouts = [];
        data.nodes.forEach(function (n) {
            if (n.spout) {
                spouts.push(n.id);
            }
            var color = n.spout ? "#FAA123" : null;
            nodes.push({
                id: n.id,
                label: n.label,
                level: n.level,
                value: n.value,
                title: (n.title && n.title.length > 0) ? n.title : undefined,
                color: color,
                hidden: n.hidden
            });
        });

        //add origin source
        if (spouts.length > 0) {
            nodes.push({
                id: 0,
                label: "",
                level: -1,
                value: 0,
                hidden: true
            });
        }

        //connect origin source to spout
        spouts.forEach(function (s) {
            edges.push({
                from: 0,
                to: s,
                value: 0,
                hidden: true
            });
        });

        var value_range = this.array_range(data.edges.map(function (e) {
            return e.value;
        }), [0,0]);

        var cycle_value_range = this.array_range(data.edges.map(function (e) {
            return e.cycleValue;
        }), [5,1000]);

        data.edges.forEach(function (e) {
            var arrow_factor = self.rangeMapper(e.value, value_range, self.edgeArrowSizeRange(), 0.25);
            var all_colors = self.edgeColorRange();
            var color_index = Math.floor(self.rangeMapper(e.cycleValue, cycle_value_range, [0, all_colors.length], -1));
            var edge_color = self.colorMapper(color_index);
            edges.push({
                id: e.id,
                from: e.from,
                to: e.to,
                value: e.value,
                title: (e.title && e.title.length) > 0 ? e.title : undefined,
                arrows: {to: {scaleFactor: arrow_factor}},
                color: {
                    color: edge_color,
                    highlight: edge_color,
                    hover: edge_color
                },
                hidden: e.hidden
            });
        });
        return {
            nodes: new vis.DataSet(nodes),
            edges: new vis.DataSet(edges)
        };
    },

    //get the min and max value in an array
    array_range: function (arr, defaultRange) {
        var min = defaultRange[0], max = defaultRange[1];
        var all_zero = true;
        arr.forEach(function (e) {
            if (e < min) {
                min = e;
            }
            if (e > max) {
                max = e;
            }
            if (e != 0) {
                all_zero = false;
            }
        });

        if (all_zero) {
            return [0, 0];
        } else {
            return [min, max];
        }
    },

    rangeMapper: function (value, in_range, out_range, defaultValue) {
        if (in_range[0] == in_range[1]) return defaultValue;
        return (value - in_range[0]) * (out_range[1] - out_range[0]) / (in_range[1] - in_range[0]) + out_range[0];
    },

    edgeArrowSizeRange: function () {
        return [0.5, 0.1];
    },

    edgeColorRange: function () {
        // "#F23030"
        return ["#17BF00", "#FF9F19", "#BB0000"];
    },

    colorMapper: function (index) {
        var colors = this.edgeColorRange();
        if (index == -1) {
            return undefined;
        } else if (index == colors.length) {
            index -= 1;
        }

        return colors[index];
    }
};

function VisTable() {

}

VisTable.prototype = {
    newData: function (data) {
        var ret = {};
        var compHead = ["Emitted", "SendTps", "RecvTps"];
        var streamHead = ["TPS", "TupleLifeCycle(ms)"];
        data.nodes.forEach(function (e) {
            ret["component-" + e.id] = {
                title: e.label,
                head: compHead,
                mapValue: e.mapValue,
                valid: true
            };
        });

        data.edges.forEach(function (e) {
            ret["stream-" + e.id] = {
                title: e.from + " -> " + e.to,
                head: streamHead,
                mapValue: e.mapValue,
                valid: true
            };
        });

        return ret;
    }
};


//ECharts
function EChart(){}
EChart.prototype = {
    newOption: function (data) {
        var isArea = false;
        if (data.name == "CpuUsedRatio" || data.name == "MemoryUsed"){
            isArea = true;
        }
        return {
            tooltip: {
                trigger: 'axis',
                formatter: function (params) {
                    var res = params[0].name + '<br/>';
                    res += '<strong>' + data.label[params[0].dataIndex] + '</strong>';
                    return res;
                },
                backgroundColor: 'rgba(255, 255, 255, 0.85)',
                borderColor: '#88BCED',
                borderWidth: 1,
                showDelay: 0,
                transitionDuration: 0.2,
                hideDelay: 0,
                textStyle: {
                    color: "#000",
                    fontSize: "10"
                },
                //change the tooltip position, to avoid content display incompletely
                position: function (p) {
                    if (p[0] > 75){
                        return [5, 5];
                    } else {
                        //console.log([width - 40, height - 40]);
                        return [p[0], p[1] - 30];
                    }
                }
            },
            grid: {
                x: 2, y: 1, x2: 2, y2: 0, borderWidth: 0
            },
            xAxis: [
                {
                    show: false,
                    type: 'category',
                    boundaryGap: false,
                    data: data.category
                }
            ],
            yAxis: [
                {
                    show: false,
                    type: 'value'
                }
            ],
            series: [
                {
                    type: 'line',
                    smooth: true,
                    symbol: 'none',
                    itemStyle: {
                        normal: {
                            color: "#3880aa",
                            areaStyle: isArea ?  {
                                type: 'default',
                                color: "#DFEDFB"
                            } : undefined,
                            lineStyle: {
                                width: isArea ? 2 : 1,
                                type: "solid",
                                color: "#88BCED"
                            }
                        }
                    },
                    data: data.data
                }
            ]
        };
    },

    init: function(selector, data){
        var self = this;
        // configure for module loader
        require.config({
            paths: {
                echarts: './assets/js/echarts'
            }
        });

        // use
        require(
            [
                'echarts',
                'echarts/chart/line'
            ],
            function (ec) {
                // Initialize after dom ready
                var myChart = ec.init(selector);
                //debugger
                // Load data into the ECharts instance
                var option = self.newOption(data);
                myChart.setOption(option);
                // we use this trick to make sure tooltip hide when mouse move out
                selector.addEventListener("mouseout", function(){
                    myChart.component.tooltip.hideTip();
                });
            }
        );
    }
};