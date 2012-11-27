var imageInterval = null;
var instanceInterval = null;
var ROWS = COLS = 4;
var ZOOM = 11;
var instanceNames = null;
var ZOOM_MAP = {
  1: 8,
  4: 9,
  16: 10,
  36: 10,
  64: 11
}
var INSTANCE_INTERVAL = 2000;
var IMAGE_INTERVAL = 500;
var STATUS_COLORS = {
  'OTHER': 'red',
  'PROVISIONING': 'orange',
  'STAGING': 'yellow',
  'RUNNING': 'green'
}


$(document).ready(function() {
  init();
});

function init() {
  $('.btn').button();
  instanceNames = [];
  initButtons();
}

function initButtons() {
  // Start button click
  $('#start').click(function() {
    $('#start').addClass('disabled');
    $('#instances').empty();
    var numInstances = parseInt($('#num-instances').val(), 10);
    createInstanceDisplay(numInstances);
    $.ajax({
      type: 'POST',
      url: '/instance',
      data: { 'num_instances': numInstances },
      dataType: 'json'
    });
    instanceInterval = setInterval(
        'instances(' + numInstances + ')', INSTANCE_INTERVAL);
  });

  // Add the file to cloud storage to let VMs know to gen tile
  $('#fractal').click(function() {
    $('#fractal').addClass('disabled');
    var numInstances = parseInt($('#num-instances').val(), 10);
    createImageTiles(numInstances);
    $.ajax({
      type: 'POST',
      url: '/start'
    });
    imageInterval = setInterval(
        'displayImages(' + numInstances + ')', IMAGE_INTERVAL);
  });

  $('#reset').click(function() {
    $('#images').empty();
    $('#fractal').addClass('disabled');
    $.ajax({
      type: 'POST',
      url: '/cleanup'
    });
    instanceInterval = setInterval('instances(0)', INSTANCE_INTERVAL);
  });
}

function createInstanceDisplay(numInstances) {
  var rows = cols = Math.sqrt(numInstances);
  // Dynamically display instance names on HTML
  for (var i = 0; i < cols; i++) {
    var column = document.createElement('div');
    column.className = 'span1';
    for (var j = 0; j < rows; j++) {
      var instanceShortName = i + '-' + j;
      var instanceLongName = 'image-processor-' + instanceShortName;
      instanceNames.push(instanceLongName);

      var color = document.createElement('div');
      color.className = 'color-block red';
      color.id = instanceLongName;
      $(column).append(color);

      var instanceElement = document.createElement('div');
      $(instanceElement).text(instanceShortName);
      $(column).append(instanceElement);
    }
    $('#instances').append(column);
  }
}

function createImageTiles(numInstances) {
  var rows = cols = Math.sqrt(numInstances);
  var zoom = ZOOM_MAP[numInstances];
  // Create image elements on page
  for (var i = 0; i < rows; i++) {
    for (var j = 0; j < cols; j++) {
      var image = document.createElement('img');
      image.id = 'image-' + zoom + '-' + i + '-' + j;
      if (j == 0) {
        image.style.clear = 'left';
      }
      $('#images').append(image);
    }
  }
}

function instances(requiredRunning) {
  $.ajax({
    url: '/instance',
    dataType: 'json',
    success: function (data) {
      if (data.hasOwnProperty('error')) {
        alert('An error occurred. Please refresh the page and try again.');
        clearInterval(instanceInterval);
        return;
      }
      var instanceCount = 0;
      for (var i = 0; i < instanceNames.length; i++) {
        var instanceName = instanceNames[i];
        var jqueryId = '#' + instanceName;
        if (data.hasOwnProperty(instanceName)) {
          var status = data[instanceName];
          if (status == 'RUNNING') {
            colorize(jqueryId, STATUS_COLORS['RUNNING']);
            instanceCount++;
          } else if (status == 'STAGING' && requiredRunning != 0) {
            colorize(jqueryId, STATUS_COLORS['STAGING']);
          } else if (status == 'PROVISIONING' && requiredRunning != 0) {
            colorize(jqueryId, STATUS_COLORS['PROVISIONING']);
          } else {
            colorize(jqueryId, STATUS_COLORS['OTHER']);
          }
        } else {
          colorize(jqueryId, STATUS_COLORS['OTHER']);
        }
      }
      if (instanceCount == requiredRunning) {
        clearInterval(instanceInterval);
        if (requiredRunning != 0) {
          $('#fractal').removeClass('disabled');
          $('#reset').removeClass('disabled');
        } else {
          instanceNames = [];
          $('#start').removeClass('disabled');
          $('#reset').addClass('disabled');
        }
      }
    }
  });
}

function colorize(jqueryId, color) {
  for (var status in STATUS_COLORS) {
    $(jqueryId).removeClass(STATUS_COLORS[status]);
  }
  $(jqueryId).addClass(color);
}

function displayImages(numInstances) {
  // Ping cloud storage to get images
  $.ajax({
    url: 'http://gce-fractal-demo.commondatastorage.googleapis.com?prefix=output/',
    dataType: 'xml',
    success: function(xml) {
      var url = 'http://commondatastorage.googleapis.com/gce-fractal-demo/';
      var imageCount = 0;
      $(xml).find('Contents').each(function() {
        var imagePath = $(this).find('Key').text();
        // Key = output/<image>.png
        var elemId = imagePath.replace('.png', '').replace('output/', '');
        document.getElementById(elemId).src = url + imagePath;
        imageCount++;
        if (imageCount == numInstances) {
          clearInterval(imageInterval);
        }
      });
    }
  });
}
