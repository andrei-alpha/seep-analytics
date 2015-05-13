function getGaugeOptions() {
  return {
    chart: {
      type: 'solidgauge'
    },

    title: null,

    pane: {
      center: ['50%', '85%'],
      size: '140%',
      startAngle: -90,
      endAngle: 90,
      background: {
        backgroundColor: (Highcharts.theme && Highcharts.theme.background2) || '#EEE',
        innerRadius: '60%',
        outerRadius: '100%',
        shape: 'arc'
      }
    },

    tooltip: {
      enabled: false
    },

    // the value axis
    yAxis: {
      stops: [
        [0.1, '#55BF3B'], // green
        [0.5, '#DDDF0D'], // yellow
        [0.9, '#DF5353'] // red
      ],
      lineWidth: 0,
      minorTickInterval: null,
      tickPixelInterval: 400,
      tickWidth: 0,
      title: {
        y: -70
      },
      labels: {
        y: 16
      },
      min: 0,
      max: 100
    },

    plotOptions: {
      solidgauge: {
        dataLabels: {
          y: 5,
          borderWidth: 0,
          useHTML: true
        }
      }
    }
  }
};

function getCpuChartData() {
  return {
    yAxis: {
        title: {
            text: 'CPU Usage'
        }
    },
    credits: {
        enabled: false
    },
    exporting: {
      enabled: false
    },
    series: [{
        name: 'cpu',
        data: [1],
        dataLabels: {
            format: '<div style="text-align:center"><span style="font-size:25px;color:' +
                ((Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black') + '">{y:.0f}</span><br/>' +
                   '<span style="font-size:12px;color:silver">% cluster usage</span></div>'
        },
        tooltip: {
            valueSuffix: '%'
        }
    }]
  }
}

function getMemoryChartData() {
  return {
    yAxis: {
      title: {
          text: 'Memory Usage'
      }
    },
    credits: {
        enabled: false
    },
    exporting: {
      enabled: false
    },
    series: [{
        name: 'Memory',
        data: [1],
        dataLabels: {
            format: '<div style="text-align:center"><span style="font-size:25px;color:' +
                ((Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black') + '">{y:.0f}</span><br/>' +
                   '<span style="font-size:12px;color:silver">% cluster usage</span></div>'
        },
        tooltip: {
            valueSuffix: '%'
        }
    }]
  }
}

function getChartData(dataset, categories) {
  return {
    labels: categories,
    datasets: [
      {
        label: "Events per second",
        fillColor: "rgba(220,220,220,0.2)",
        strokeColor: "rgba(299,115,115,1)",
        pointColor: "rgba(299,95,95,1)",
        pointStrokeColor: "#fff",
        pointHighlightFill: "#fff",
        pointHighlightStroke: "rgba(220,220,220,1)",
        data: dataset
      }
    ]
  }
}

function getThroughputData(dataset, categories) {
  return {
    colors: ['#000099', '#006600', '#660099'],
    chart: {
        type: 'column'
    },
    title: {
        text: 'Cluster Throughput'
    },
    subtitle: {
        text: 'SEEP workloads'
    },
    xAxis: {
        title: {
            text: 'streaming jobs running concurrently'
        },
        categories: categories,
        crosshair: true
    },
    yAxis: {
        min: 0,
        title: {
            text: 'events per second'
        }
    },
    credits: {
        enabled: false
    },
    tooltip: {
        headerFormat: '<span style="font-size:10px">{point.key} queries</span><table>',
        pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
            '<td style="padding:0"><b>{point.y:.1f} events / second</b></td></tr>',
        footerFormat: '</table>',
        shared: true,
        useHTML: true
    },
    plotOptions: {
        column: {
            pointPadding: 0.2,
            borderWidth: 0
        }
    },
    series: dataset
  }
}

function getHighchartData(title, labels, data) {
  return {
    /*chart: {
      type: 'pline',
      zoomType: 'x'
    },*/
    colors: ['rgba(299,115,115,1)'],
    title: {
      align: 'left',
      text: title,
      style: {"fontSize": "12px" }
    },
    legend: {
      enabled: false
    },
    xAxis: {
      categories: labels,
      labels: {
        formatter: function() {
          var date = new Date(this.value);
          return ('0' + date.getHours()).slice(-2) + ":" + ('0' + date.getMinutes()).slice(-2)
        }
      }
    },
    yAxis: {
      title: {
        text: 'events per second'
      },
      min: 0
    },
    credits: {
        enabled: false
    },
    plotOptions: {
      spline: {
        marker: {
            enabled: true
        }
      }
    },
    tooltip: {
      headerFormat: '',
      pointFormat: '{point.y:.2f} events per second'
    },
    exporting: {
      enabled: false
    },
    series: [{
      name: 'events per second',
      data: data,
      lineWidth: 1.5,
      marker: {
       enabled: true,
       symbol: 'circle',
       radius: 3
      },
      tooltip: {
        valueDecimals: 1
      }
    }]
  }
}