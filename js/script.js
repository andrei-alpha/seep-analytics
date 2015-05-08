var globalDataset;
var currentView = window.location.pathname, previousView = '';
var dataFilters = {
  '1-minute rate': [10, 1],
  '5-minute rate': [20, 2],
  '15-minute rate': [40, 4],
  'mean rate': [20, 2],
  'default': [10, 1] 
}
var graphTitles = ['1-minute rate', '5-minute rate', '15-minute rate', 'mean rate',
  '1-minute rate-avg', '5-minute rate-avg', '15-minute rate-avg', 'mean rate-avg',
  '1-minute rate-fairness', '5-minute rate-fairness', '15-minute rate-fairness', 'mean rate-fairness'];
var lineChartIds = {}

function dataSelect(dataset, filter) {
  selectedData = []
  for (var i = Math.max(0, dataset.length - filter[0]); i < dataset.length; i += filter[1])
    selectedData.push(dataset[i]);
  return selectedData;
}

function getMokeData() {
  var mokeData = {
    labels: [],
    datasets: [
      {
        label: "Events per second",
        fillColor: "rgba(220,220,220,0.2)",
        strokeColor: "rgba(299,115,115,1)",
        pointColor: "rgba(299,95,95,1)",
        pointStrokeColor: "#fff",
        pointHighlightFill: "#fff",
        pointHighlightStroke: "rgba(220,220,220,1)",
        data: []
      }
    ]
  };
  var time = new Date().getTime();
  var data = mokeData;
  
  data['labels'] = []
  data['datasets'][0]['data'] = []
  for (var i = 9; i > 0; --i) {
    label = new Date(time - i * 60000);
    data['labels'].push(('0' + label.getHours())/slice(-2) + ":" + ('0' + label.getMinutes()).slice(-2)) 
    data['datasets'][0]['data'].push(Math.ceil(Math.random() * 50))
  }
  console.log(data)
  return data
}

function getChartData(dataset, labelsData) {
  return {
    labels: labelsData,
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

function populateGraph(id, labels, data) {
  var container = $('#' + id).get()[0].getContext("2d");
  var chartData = getChartData(data, labels)
  var lineChart = new Chart(container).Line(chartData, {scaleBeginAtZero: true});
  lineChartIds[id] = lineChart;
}

function shallowPopulateGraphs(id, labels, data) {
  var lineChart = lineChartIds[id];
  if (lineChart == undefined || lineChart.datasets[0].points.length == 0) {
    populateGraph(id, labels, data)
    return;
  }
  var chartData = getChartData(data, labels)

  while(lineChart.datasets[0].points.length && lineChart.datasets[0].points[0].label != chartData.labels[0] || 
    (lineChart.datasets[0].points.length > 1 && lineChart.datasets[0].points[1].label != chartData.labels[1])) {
    lineChart.removeData();
  }

  for (var i = lineChart.datasets[0].points.length; i < chartData.datasets[0].data.length; ++i) {
    value = chartData.datasets[0].data[i];
    label = chartData.labels[i];
    lineChart.addData([value], label);
  }
  
  var needsUpdate = false;
  for (var i = 0; i < chartData.datasets[0].data.length; ++i) {
    if (chartData.datasets[0].data[i] != lineChart.datasets[0].points[i].value) {
      lineChart.datasets[0].points[i].value = chartData.datasets[0].data[i];
      needsUpdate = true;
    }
  }

  if (needsUpdate == true)
    lineChart.update()
}

function getDataset() {
  $.ajax({
    url: "/backend/dataset",
    type: "get",
    success: function(response){
      globalDataset = response;
      openView(currentView);
      //updateGraphs(globalDataset[currentView], currentView);
    },
  });
}

function updateGraphs(dataset, graphType) {
  var shallowUpdate = true;
  if (previousView != currentView) {
    previousView = currentView;
    shallowUpdate = false;
    $('#graphs tr').remove();
  }

  // Load the apps / containers in reverse so the newest one is at top
  var keys = new Array();
  for (var k in dataset) {
    keys.push(k);
  }
  keys = keys.sort();

  for (var kind = keys.length - 1; kind >= 0; --kind) {
    var key = keys[kind];
    var item = dataset[key]
    var type = item['type'];
    var metric = item['metric']

    // Don't show source since everthing is 0
    if (type == 'Source')
      continue;

    var labels = []
    var data = {}
    var lastEventTimestamp = 0;

    for (var j = 0; j < item['data'].length; ++j) {
      for (var label in item['data'][j]) {
        if (label == 'time') {
          // python timestamp is in seconds so we need to transform in miliseconds
          var date = new Date(item['data'][j][label] * 1000);
          labels.push(('0' + date.getHours()).slice(-2) + ":" + ('0' + date.getMinutes()).slice(-2))

          if (date.getTime() > lastEventTimestamp)
            lastEventTimestamp = date.getTime();
        } else {
          if (data[label] == null)
            data[label] = []
          data[label].push(item['data'][j][label]);
        }
      }
    }

    if (labels.length == 0)
      continue;

    titles = []
    ids = []
    for (var i = 0; i < graphTitles.length; ++i) {
      var title = graphTitles[i];
      if (!(title in data))
        continue;

      titles.push(title)
      ids.push("graph-" + key + "-" + title.replace(/\s+/g, ''));
    }

    var graphName;
    if (graphType == 'containers')
      graphName = '<b>' + type + '</b>' +  ' for app: ' + item['app'];
    else if(graphType == 'apps') {
      appName = parseInt(key.split("_")[1]);
      graphName = 'Application <b>' + appName + '</b> ' + item['query'];

      if ((new Date()).getTime() - lastEventTimestamp < 600000)
        graphName += String(appName) + '<img style="margin-left: 15px;" width="18px" src="static/css/loading.gif"></img>';
    }
    else
      graphName = 'Wombat Cluster <b>' + key + '</b>';

    var doShallowPopulate = true;
    if (!shallowUpdate || $('#' + ids[0]).length == 0) {
      var graphsTemplate = $('#graphsTemplate').html();
      var template = graphsTemplate.format(graphName, titles[0], ids[0], titles[1], ids[1], titles[2], ids[2], titles[3], ids[3], metric);
      $('#graphs > tbody:last').append(template);
      doShallowPopulate = false;
    }

    for (var i = 0; i < ids.length; ++i) {
      var filter;
      if (titles[i] in dataFilters)
        filter = dataFilters[titles[i]]
      else
        filter = dataFilters['default']
      
      if (doShallowPopulate)
        shallowPopulateGraphs(ids[i], dataSelect(labels, filter), dataSelect(data[titles[i]], filter));
      else
        populateGraph(ids[i], dataSelect(labels, filter), dataSelect(data[titles[i]], filter));
    }
  }
}

$(function() {
  getDataset();
  setInterval(function() {
    getDataset();
  }, 10000);
})

function openView(view) {
  if (view.contains('containers'))
    openContainersView();
  else if(view.contains('apps'))
    openAppsView();
  else if(view.contains('cluster'))
    openClusterView();
  else if(view.contains('admin'))
    openAdminView();
  else // Default view is apps
    openAppsView();
}

function openClusterView() {
  window.history.pushState({}, "", "/cluster");

  $('#admin-console').css("display", "none");
  $('#graphs').css("display", "block");
  currentView = 'cluster';
  updateGraphs(globalDataset[currentView], currentView);
}

function openAppsView() {
  window.history.pushState({}, "", "/apps");

  $('#admin-console').css("display", "none");
  $('#graphs').css("display", "block");
  currentView = 'apps';
  updateGraphs(globalDataset[currentView], currentView);
}

function openContainersView() {
  window.history.pushState({}, "", "/containers");

  $('#admin-console').css("display", "none");
  $('#graphs').css("display", "block");
  currentView = 'containers';
  updateGraphs(globalDataset[currentView], currentView);
}

function openAdminView() {
  window.history.pushState({}, "", "/admin");

  $('#graphs').css("display", "none");
  $('#admin-console').css("display", "block");
  previousView = currentView;
  currentView = 'admin';
  getAvailableOptions();
}

//first, checks if it isn't implemented yet
if (!String.prototype.format) {
  String.prototype.format = function() {
    var args = arguments;
    return this.replace(/{(\d+)}/g, function(match, number) {
      return typeof args[number] != 'undefined'
        ? args[number]
        : match
      ;
    });
  };
}

if (!String.prototype.contains) {
  String.prototype.contains = function(it) { 
    return this.indexOf(it) != -1; 
  };
}