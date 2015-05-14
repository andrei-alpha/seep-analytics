
function getProgress(count) {
  if (count > 50)
    return;

	$.ajax({
  	url: "/command/status",
  	type: "get",
  	success: function(response) {
  		var progress = parseInt(response['progress'])
  		var current = response['current']
  		$('#progress-bar').css('width', progress + "%");
  		$('#progress-bar').text(progress + "%");
  		$('#progress-label').text(current);

  		if (progress != 100) {
  			setTimeout(function() {
  				getProgress(count + 1)
 				}, 500);
 			} else {
 				$('#progress-bar').css('width', "100%");
 				setTimeout(function() {
 					$('#progress').hide();
 				}, 2000)
 			}
  	}
  });
}

function sendCommand(command, data) {
	$('#progress').css('display', 'block');
	$('#progress-bar').css('width', "0%");

	$.ajax({
    url: "/command/" + command,
    data: data,
    type: "post",
    success: function(response) {
    	if (response == 'ok')
    		getProgress(0);
    	else
    		$('#progress-label').text('Failed');
    }
  });
}

function bytesToSize(bytes) {
  var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  if (bytes == 0) return '0 Byte';
  var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
  return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
};

function numberToString(number) {
  var sizes = ['', 'K', 'M', 'B'];
  if (number == 0) return '0';
  var i = parseInt(Math.floor(Math.log(number) / Math.log(1000)));
  return Math.round(number / Math.pow(1000, i), 2) + ' ' + sizes[i];
};

function updateAdminTable(data) {
  $('#td-current-rate').text(parseInt(data['current_rate']) + ' events / sec');
  $('#td-total-events').text(numberToString(data['total_events']) + ' events');
  $('#tb-kakfa-logs').text(bytesToSize(data['kafka_logs']));
  $('#tb-hadoop-logs').text(bytesToSize(data['hadoop_logs']));
  $('#tb-total-mem').text(bytesToSize(data['total_mem']));
  $('#tb-total-cpus').text(data['total_cpus']);

  var cpu_graph = $('#cpu-graph').highcharts();
  if (cpu_graph) {
    point = cpu_graph.series[0].points[0];
    point.update(data['cpu_usage']);
  }
  var mem_graph = $('#memory-graph').highcharts();
  if (mem_graph) {
    point = mem_graph.series[0].points[0];
    point.update(data['mem_usage']);
  }
}

function updateResourcesGraphs(data) {
  var chart;
  var series = {'cpu': [], 'ram': [], 'net_io': [], 'disk_io': []};
  var categories = [];

  var cnt = 0;
  for (host in data['hosts']) {
    categories.push(host);
    
    for (var i = 0; i < data['hosts'][host]['cpu'].length; ++i) {
      point = {};
      point.y = data['hosts'][host]['cpu'][i];
      point.color = colorGradient(point.y);
      if (cnt == 0) {
        series['cpu'].push({'name': 'cpu' + (1 + i), 'data': [point]});
      } else {
        series['cpu'][i]['data'].push(point);
      }
    }

    point = {};
    point.y = data['hosts'][host]['memory'][1];
    point.color = colorGradient(point.y);
    if (cnt == 0) {
      series['ram'].push({'name': 'ram', 'data': [point]});
    } else {
      series['ram'][0]['data'].push(point);
    }

    for (var i = 0; i < data['hosts'][host]['net_io'].length; ++i) {
      point = {};
      point.y = data['hosts'][host]['net_io'][i];
      point.color = (i == 0 ? '#7CB5EC' : '#FF7373')
      if (cnt == 0) {
        series['net_io'].push({'name': (i == 0 ? 'bytes_send' : 'bytes_recv'), 'data': [point]});
      } else {
        series['net_io'][i]['data'].push(point);
      }
    }

    for (var i = 0; i < data['hosts'][host]['disk_io'].length; ++i) {
      point = {};
      point.y = data['hosts'][host]['disk_io'][i];
      point.color = (i == 0 ? '#7CB5EC' : '#FF7373')
      if (cnt == 0) {
        series['disk_io'].push({'name': (i == 0 ? 'read_bytes' : 'write_bytes'), 'data': [point]});
      } else {
        series['disk_io'][i]['data'].push(point);
      }
    }

    ++cnt;
  }

  var timestamp = new Date().getTime();
  /* Cpu graphs */
  chart = $('#cpu-bars-graph').highcharts();
  if (chart) {
    for (var i = 0; i < chart.series.length; ++i) {
      chart.series[i].setData(series['cpu'][i]['data']);
    }
  } else {
    $('#cpu-bars-graph').highcharts(getBarsChartData(series['cpu'], categories, 'Nodes cpu utilization', '% percent cpu used'));
  }
  chart = $('#cpu-area-graph').highcharts();
  if (chart == undefined) {
    $('#cpu-area-graph').highcharts(getAreaChartData('Cluster cpu utilization', '% percent cpu used', [{'name': 'CPU utilization', 'data': []}]));
    chart = $('#cpu-area-graph').highcharts();
  }
  chart.series[0].addPoint([timestamp, data['overall']['cpu_usage']])

  /* Memory graphs */
  chart = $('#ram-bars-graph').highcharts();
  if (chart) {
    for (var i = 0; i < chart.series.length; ++i) {
      chart.series[i].setData(series['ram'][i]['data']);
    }
  } else {
    $('#ram-bars-graph').highcharts(getBarsChartData(series['ram'], categories, 'Nodes ram utilization', '% percent ram used'));
  }
  chart = $('#ram-area-graph').highcharts();
  if (chart == undefined) {
    $('#ram-area-graph').highcharts(getAreaChartData('Cluster RAM utilization', '% percent ram used', [{'name': 'RAM utilization', 'data': []}]));
    chart = $('#ram-area-graph').highcharts();
  }
  chart.series[0].addPoint([timestamp, data['overall']['mem_usage']]);

  /* Network IO graphs */
  chart = $('#network-bars-graph').highcharts();
  if (chart) {
    for (var i = 0; i < chart.series.length; ++i) {
      chart.series[i].setData(series['net_io'][i]['data']);
    }
  } else {
    $('#network-bars-graph').highcharts(getBarsChartData(series['net_io'], categories, 'Nodes network utilization', 'network io per second', 'rate'));
  }
  chart = $('#network-area-graph').highcharts();
  if (chart == undefined) {
    $('#network-area-graph').highcharts(getAreaChartData('Cluster network utilization', 'network io per second', [{'name': 'send', 'data': []}, {'name': 'read', 'data': []}], 'rate'));
    chart = $('#network-area-graph').highcharts();
  }
  chart.series[0].addPoint([timestamp, data['overall']['net_io'][0]]);
  chart.series[1].addPoint([timestamp, data['overall']['net_io'][1]]);

  /* Disk IO graphs */
  chart = $('#disk-bars-graph').highcharts();
  if (chart) {
    for (var i = 0; i < chart.series.length; ++i) {
      chart.series[i].setData(series['disk_io'][i]['data']);
    }
  } else {
    $('#disk-bars-graph').highcharts(getBarsChartData(series['disk_io'], categories, 'Nodes disk utilization', 'disk per io second', 'rate'));
  }
  chart = $('#disk-area-graph').highcharts();
  if (chart == undefined) {
    $('#disk-area-graph').highcharts(getAreaChartData('Cluster disk utilization', 'disk io per second', [{'name': 'reads', 'data': []}, {'name': 'writes', 'data': []}], 'rate'));
    chart = $('#disk-area-graph').highcharts();
  }
  chart.series[0].addPoint([timestamp, data['overall']['disk_io'][0]]);
  chart.series[1].addPoint([timestamp, data['overall']['disk_io'][1]]);
}

function getClusterInfo() {
  $.ajax({
    url: "/command/resource_report",
    type: "get",
    success: function(response) {
      if (response != 'pending') {
        updateAdminTable(response['overall']);
        updateResourcesGraphs(response);
      }
    }
  });
}

function submitSeepQuery() {
	var data = {'queryName': $("#available-queries option:selected").text(),
		'deploymentSize': $("#deployment-size option:selected").text()}
  sendCommand('submit_query', data);
}

function killAllSeepQueries() {
	sendCommand('kill_all_seep', {});
}

function updateSEEP() {
  var data = {'branch': $("#seep-branches option:selected").text()}
	sendCommand('update_seep', data);
}

function updateAnalytics() {
  var data = {'branch': $("#analytics-branches option:selected").text()}
	sendCommand('update_analytics', data)
}

function clearKafkaLogs() {
  sendCommand('clear_kafka_logs', {});
}

function clearHadoopLogs() {
  sendCommand('clear_hadoop_logs', {});
}

function getAvailableOptions() {
	$.ajax({
		dataType: "json",
    url: "/options",
    type: "get",
    success: function(response) {
    	for (key in response) {
        for (var i = 0; i < response[key].length; ++i) {
    		  $('#' + key).append('<option class="option">' + response[key][i] + '</option>')
        }
      }
    }
  });
}