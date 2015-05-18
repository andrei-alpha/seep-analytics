
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

  var keys = new Array();
  for (var k in data['hosts']) {
    keys.push(k);
  }
  keys = keys.sort();

  var cnt = 0;
  for (var idk = 0; idk < keys.length; ++idk) {
    host = keys[idk];

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
        series['net_io'].push({'name': (i == 0 ? 'network sends' : 'network recvs'), 'data': [point]});
      } else {
        series['net_io'][i]['data'].push(point);
      }
    }

    for (var i = 0; i < data['hosts'][host]['disk_io'].length; ++i) {
      point = {};
      point.y = data['hosts'][host]['disk_io'][i];
      point.color = (i == 0 ? '#7CB5EC' : '#FF7373')
      if (cnt == 0) {
        series['disk_io'].push({'name': (i == 0 ? 'disk reads' : 'disk writes'), 'data': [point]});
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
    $('#cpu-bars-graph').highcharts(getBarsChartData(series['cpu'], categories, 'Nodes cpu(s) utilization', '% percent cpu used'));
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
    $('#network-area-graph').highcharts(getAreaChartData('Cluster network utilization', 'network io per second', [{'name': 'network sends', 'data': []}, {'name': 'network recvs', 'data': []}], 'rate'));
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
    $('#disk-bars-graph').highcharts(getBarsChartData(series['disk_io'], categories, 'Nodes disk utilization', 'disk io per second', 'rate'));
  }
  chart = $('#disk-area-graph').highcharts();
  if (chart == undefined) {
    $('#disk-area-graph').highcharts(getAreaChartData('Cluster disk utilization', 'disk io per second', [{'name': 'disk reads', 'data': []}, {'name': 'disk writes', 'data': []}], 'rate'));
    chart = $('#disk-area-graph').highcharts();
  }
  chart.series[0].addPoint([timestamp, data['overall']['disk_io'][0]]);
  chart.series[1].addPoint([timestamp, data['overall']['disk_io'][1]]);
}

function updateOperator(id, data) {
  var chart = $('#' + id + '-chart').highcharts();
  chart.series[0].addPoint(data['cpu_percent']);
  $('#' + id).css('opacity', '1');
  $('#' + id + '-cm').text('cpu: {0}% ram: {1}%'.format(data['cpu_percent'], parseFloat(data['memory_percent']).toFixed(2)));
  $('#' + id + '-pid').text('pid: {0}'.format(data['pid']));
  $('#' + id).css('background-color', colorGradientOperator(data['cpu_percent']));
}

function colorGradientOperator(value) {
  var colors = ['#BCF0B6', '#F5F55D', '#F08D3C', '#E85A5A'];
  return colors[Math.min(Math.max(parseInt(value / 25), 0), 3)];
}

function markDeadWorker(id) {
  $('#' + id + '-cm').text('dead');
  $('#' + id + '-pid').text('dead');
  $('#' + id).css('background-color', '#FFCACA');
  $('#' + id).css('opacity', '0.6');
}

function updateOperatorsTable(hosts) {
  if (hosts == undefined || hosts.length)
    return;
  for (id in liveOperatorsById) {
    liveOperatorsById[id] = false;
  }

  var keys = new Array();
  for (k in hosts) {
    keys.push(k);
  }
  keys = keys.sort();

  for (var i = 0; i < keys.length; ++i) {
    var host = hosts[keys[i]];
    if (host['workers'].length == 0)
      continue;

    hostName = host['host'].replace(".", "-");
    if ($('#operators-table-' + hostName).length == 0) {
      var template = $('#operatorsHostTemplate').html();
      var html = template.format(hostName);
      $('#operators-tab').append(html);
    }

    if (!(hostName in addedOperatorsCountPerHost)) {
      addedOperatorsCountPerHost[hostName] = 0;
    }

    for (var j = 0; j < host['workers'].length; ++j) {
      var worker = host['workers'][j];

      var workerId = hostName + '-W' + worker['data.port'];
      liveOperatorsById[workerId] = true;

      if ($('#' + workerId).length == 0) {

        // Add a new row to the table if 6 items were placed
        if (addedOperatorsCountPerHost[hostName] % 6 == 0) {
          var x = addedOperatorsCountPerHost[hostName];
          var template = $('#operatorsTableRowTemplate').html();
          var html = template.format(hostName, x, x+1, x+2, x+3, x+4, x+5);
          $('#operators-table-' + hostName + ' > tbody:last').append(html);
        }

        var id = 'owt-' + hostName + '-' + addedOperatorsCountPerHost[hostName];
        var template = $('#operatorTemplate').html();
        var icon = '<span class="glyphicon glyphicon-cog" aria-hidden="true"></span>'
        var data_port = (worker['data.port'] != undefined ? worker['data.port'] : 'x')
        var title = icon + (worker['type'] != undefined ? worker['type'] : 'Generic') + ' Operator ' + data_port
        var html = template.format(workerId, title, worker['cpu_percent'], parseFloat(worker['memory_percent']).toFixed(2), worker['pid'])
        $('#' + id).html(html);
        $('#' + workerId).css('background-color', colorGradientOperator(worker['cpu_percent']));
        $('#' + workerId + '-chart').highcharts( getSparkLineData('cpu', 'x', [{'name': 'cpu', 'data': [worker['cpu_percent']]}]) );

        addedOperatorsCountPerHost[hostName] += 1;
      } else {
        updateOperator(workerId, worker);
      }
    }
  }

  for (id in liveOperatorsById) {
    if (liveOperatorsById[id] == false) {
      markDeadWorker(id);
    }
  }
}

function getClusterInfo() {
  $.ajax({
    url: "/command/resource_report",
    type: "get",
    success: function(response) {
      if (response != 'pending') {
        if (currentView == 'cluster')
          updateAdminTable(response['overall']);
        if (currentView == 'resources') {
          updateResourcesGraphs(response)
          updateOperatorsTable(response['hosts']);
        }
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