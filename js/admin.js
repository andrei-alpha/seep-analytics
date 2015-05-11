
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

function getClusterInfo(count) {
  if (count > 50)
    return;

  $.ajax({
    url: "/command/get_info/status",
    type: "get",
    success: function(res) {
      if (res == 'pending') {
        setTimeout(function() {
          getClusterInfo(count + 1)
        }, 500);
      } else {
        $('#td-current-rate').text(parseInt(res['current_rate']) + ' events / sec');
        $('#td-total-events').text(numberToString(res['total_events']) + ' events');
        $('#tb-kakfa-logs').text(bytesToSize(res['kafka_logs']));
        $('#tb-hadoop-logs').text(bytesToSize(res['hadoop_logs']));
        $('#tb-total-mem').text(bytesToSize(res['total_mem']));
        $('#tb-total-cpus').text(res['total_cpus']);

        var cpu_graph = $('#cpu-graph').highcharts();
        if (cpu_graph) {
          point = cpu_graph.series[0].points[0];
          point.update(res['cpu_usage']);
        }
        var mem_graph = $('#memory-graph').highcharts();
        if (mem_graph) {
          point = mem_graph.series[0].points[0];
          point.update(res['mem_usage']);
        }
      }
    }
  });
}

function askClusterInfo() {
  $.ajax({
    url: "/command/get_info",
    type: "post",
    success: function(res) {
      getClusterInfo(0);
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