
function getProgress() {
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
  				getProgress()
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
    		getProgress();
    	else
    		$('#progress-label').text('Failed');
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
	sendCommand('update_seep', {});
}

function updateAnalytics() {
	sendCommand('update_analytics')
}

function getAvailableQueries() {
	$.ajax({
		dataType: "json",
    url: "/command/get_queries",
    type: "post",
    success: function(response) {
    	for (var i = 0; i < response.length; ++i)
    		$('#available-queries').append('<option class="option">' + response[i] + '</option>')
    }
  });
}