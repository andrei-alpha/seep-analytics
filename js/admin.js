
function getProgress() {
	$.ajax({
  	url: "/command/status",
  	type: "get",
  	success: function(response) {
  		var progress = parseInt(response['progress'])
  		var current = response['current']

  		console.log(current, progress);

  		if (progress != 100 || current != '') {
  			setTimeout(function() {
  				getProgress()
 				}, 500);
 			}
  	}
  });
}

function sendComand(command, data) {
	$.ajax({
    url: "/command/" + command,
    data: data,
    type: "post",
  });
  getProgress();
}

function submitSeepQuery() {
  console.log('here');
}

function killAllSeepQueries() {
	sendComand('kill_all_seep', {});
}