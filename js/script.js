var globalDataset;
var currentView = window.location.pathname, previousView = '';
var dataFilters = {
  '1-minute rate': 1,
  '5-minute rate': 2,
  '5-minute rate-fairness': 2,
  '5-minute rate-avg': 2,
  '15-minute rate': 4,
  '15-minute rate-fairness': 4,
  '15-minute rate-avg': 4,
  'mean rate': 4,
  'mean rate-fairness': 4,
  'mean rate-avg': 4,
  'default': 1 
}
var graphTitles = ['1-minute rate', '5-minute rate', '15-minute rate', 'mean rate',
  '1-minute rate-avg', '5-minute rate-avg', '15-minute rate-avg', 'mean rate-avg',
  '1-minute rate-fairness', '5-minute rate-fairness', '15-minute rate-fairness', 'mean rate-fairness'];
var addedOperatorsCountPerHost = {};
var liveOperatorsById = {};

/*
Performs data aggregation by the given interval
*/
function dataSelect(title, labels, dataset) {
  var filter;
  if (title in dataFilters)
    filter = dataFilters[title];
  else
    filter = dataFilters['default'];

  selectedData = []
  selectedLabels = []
  var MaxItems = 10;
  var previousTimestamp = -1;
  var start = 0;
  
  for (var i = 0; i < labels.length; ++i) {
    if (previousTimestamp != -1 && labels[i] - previousTimestamp > 5 * 60 * 1000 /* 5 minutes */ ) {
      start = i;
    }
    previousTimestamp = labels[i];
  }
  var error_range = 0;
  if (dataset.length > 1)
    error_range = 1;
  for (var i = Math.max(start, dataset.length - MaxItems * filter); i < dataset.length - error_range; i += filter) {
    var sum = 0;
    for (var j = 0; j < filter && i + j < dataset.length; ++j)
      sum += dataset[i + j];
    var avg = sum / Math.min(filter, dataset.length - i);
    selectedData.push(avg);
    selectedLabels.push(labels[Math.min(i + filter - 1, labels.length - 1)]);
  }
  return [selectedLabels, selectedData];
}

function populateGraph(id, title, data) {
  $('#' + id).highcharts(getHighchartData(title, data[0], data[1]));
}

function updateGraph(id, title, data) {
  var chart = $('#' + id).highcharts();

  if (chart == undefined || chart.series[0].data.length == 0) {
    populateGraph(id, title, data);
  }

  var needsUpdate = false;
  for (var i = 0; i < chart.series[0].data.length; ++i) {
    if (chart.series[0].data[i].y != data[1][i] || chart.xAxis[0].categories[i] != data[0][i]) {
      needsUpdate = true;
    }
  }

  if (needsUpdate) {
    chart.series[0].setData(data[1]);
    chart.xAxis[0].setCategories(data[0]);
  } else if(data[1].length > chart.series[0].data.length) {
    chart.series[0].addPoint(data[1][data[1].length-1], data[0][data[0].length-1])    
  }
}

function getDataset(isRecurrent) {
  $.ajax({
    url: "/backend/dataset",
    type: "get",
    success: function(response){
      globalDataset = response;
      openView(currentView);
      if (isRecurrent) {
        setTimeout(function() {
          getDataset(true);
        }, 10000);
      }
    },
  });
}

function addHighChart(id, series, categories) {
  $('#' + id).highcharts();
}

function updateStatistics(dataset, shallowUpdate) {
  var categories = []
  var series = [{'name': 'Cluster Throughput', 'data': []},
    {'name': 'Average Container', 'data': []}];
  for (key in dataset) {
    points = dataset[key]['tss']
    avgPerCluster = dataset[key]['total'] / Object.keys(points).length;
    avgPerContainer = dataset[key]['total'] / dataset[key]['count'];

    categories.push(key)
    series[0]['data'].push(avgPerCluster);
    series[1]['data'].push(avgPerContainer);
  }

  if (shallowUpdate == false) {
    setTimeout(function() {
      $('#stats-graph').highcharts(getThroughputData(series, categories));
      $('#cpu-graph').highcharts(Highcharts.merge(getGaugeOptions(), getCpuChartData()));
      $('#memory-graph').highcharts(Highcharts.merge(getGaugeOptions(), getMemoryChartData()));
    }, 100);
  } else {
    var chart = $('#stats-graph').highcharts();
    chart.series[0].setData(series[0]['data']);
    chart.series[1].setData(series[1]['data']);
  }
}

function colorGradient(value) {
  var colors = ['#3CBF21', '#82C121', '#C3BB20', '#C5741F', '#C72A1F'];
  return colors[Math.min(Math.max(parseInt(value / 20), 0), 4)];
}

function updateGraphs(dataset, graphType) {
  var shallowUpdate = true;
  var runningApps = 0;
  if (previousView != currentView) {
    previousView = currentView;
    shallowUpdate = false;
    $('#graphs tr').remove();

    if (currentView == "cluster") {
      var statsTemplate = $('#statisticsTemplate').html();
      var tableTemplate = $('#infoTableTemplate').html();
      var statsTemplateHtml = statsTemplate.format('stats-graph', tableTemplate);
      $('#graphs > tbody:last').append(statsTemplateHtml);
      setTimeout(function() {getClusterInfo(false); }, 100);
    }
  }

  if (graphType == 'resources') {
    if (!shallowUpdate) {
      var resourcesTemplate = $('#resourcesRowTemplate').html();
      var resourcesTemplateHtml = resourcesTemplate.format('cpu-bars-graph', 'cpu-area-graph');
      $('#graphs > tbody:last').append(resourcesTemplateHtml);
      resourcesTemplateHtml = resourcesTemplate.format('ram-bars-graph', 'ram-area-graph');
      $('#graphs > tbody:last').append(resourcesTemplateHtml);
      resourcesTemplateHtml = resourcesTemplate.format('network-bars-graph', 'network-area-graph');
      $('#graphs > tbody:last').append(resourcesTemplateHtml);
      resourcesTemplateHtml = resourcesTemplate.format('disk-bars-graph', 'disk-area-graph');
      $('#graphs > tbody:last').append(resourcesTemplateHtml);
      getClusterInfo(false);
    }
    return;
  }

  // Load the apps / containers in reverse so the newest one is at top
  var keys = new Array();
  for (var k in dataset) {
    keys.push(k);
  }
  keys = keys.sort();

  // Show statistic first
  for (var kind = keys.length - 1; kind >= 0; --kind) {
    if (keys[kind] == 'Statistics') {
      updateStatistics(dataset[keys[kind]], shallowUpdate);
    }
  }

  var countGraphs = 0;
  for (var kind = keys.length - 1; kind >= 0; --kind) {
    var key = keys[kind];
    var item = dataset[key];
    var type = item['type'];
    var metric = item['metric']

    if (key == 'Statistics')
      continue;

    // Don't show source since everthing is 0
    if (type == 'Source' || (type == undefined && graphType == "containers"))
      continue;

    var labels = []
    var data = {}
    var lastEventTimestamp = 0;

    for (var j = 0; j < item['data'].length; ++j) {
      for (var label in item['data'][j]) {
        if (label == 'time') {
          // python timestamp is in seconds / 30 so we need to transform in miliseconds
          var timestamp = item['data'][j][label] * 1000 * 30
          labels.push(timestamp);

          if (timestamp > lastEventTimestamp)
            lastEventTimestamp = timestamp;
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

      if ((new Date()).getTime() - lastEventTimestamp < 60000) {
        ++runningApps;
        graphName += String(appName) + '<img style="margin-left: 15px;" width="18px" src="static/css/loading.gif"></img>';
      }
    }
    else
      graphName = 'Wombat Cluster <b>' + key + '</b>';

    var doShallowPopulate = true;
    if (!shallowUpdate || $('#' + ids[0]).length == 0) {
      var graphsTemplate = $('#graphsTemplate').html();
      var template = graphsTemplate.format(graphName, ids[0], ids[1], ids[2], ids[3]);
      $('#graphs > tbody:last').append(template);
      doShallowPopulate = false;
    }

    for (var i = 0; i < ids.length; ++i) {
      if (doShallowPopulate) 
        updateGraph(ids[i], titles[i], dataSelect(titles[i], labels, data[titles[i]]));
      else {
        // Slow function. don't block the browser
        setTimeout(function(args) {
          populateGraph(args[0], args[1], args[2]);
        }, ++countGraphs * 40, [ids[i], titles[i], dataSelect(titles[i], labels, data[titles[i]])]);
      }
    }
  }
}

$(function() {
  getDataset(true);
  getAvailableOptions();
  getClusterInfo(true);
  getSchedulerConfigs();

  $('#scheduler-type').change(function() {
    var name1 = 'startup.scheduling.type';
    var name2 = 'runtime.scheduling.enabled';
    var value = $("#scheduler-type option:selected").val();
    if (value == "0") {
      setConfig('Scheduler', name1, 0);
      setConfig('Scheduler', name2, 0);
    } else if (value == "1") {
      setConfig('Scheduler', name1, 1);
      setConfig('Scheduler', name2, 0);
    } else if (value == "2") {
      setConfig('Scheduler', name1, 2);
      setConfig('Scheduler', name2, 0);
    } else if (value == "3") {
      setConfig('Scheduler', name1, 2);
      setConfig('Scheduler', name2, 1);
    }
  })
})

function openView(view) {
  view = view.replace(/[^a-zA-Z]/g, "");
  var views = ['admin', 'cluster', 'resources', 'apps', 'containers'];
  view = (views.indexOf(view) != -1 ? view : 'apps');
  window.history.pushState({}, '', '/' + view);
  previousView = currentView;
  currentView = view;

  if (currentView == "resources") {
    $('#nav-tabs').css("display", "block");
    $('#scheduler-options').css("display", "block");
  } else {
    addedOperatorsCountPerHost = {};
    $('#operators-tab').html('');
    $('.nav-tabs a[href="#resources-tab"]').tab('show');
    $('#scheduler-options').css("display", "none");
    $('#nav-tabs').css("display", "none");
  }

  if (currentView == 'admin') {
    $('#graphs').css("display", "none");
    $('#admin-console').css("display", "block");
  } else {
    $('#admin-console').css("display", "none");
    $('#graphs').css("display", "block");
    updateGraphs(globalDataset[currentView], currentView);
  }
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
