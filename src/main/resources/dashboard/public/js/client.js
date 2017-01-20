var map;
var markers = [];
var incidents = [];
var violations = [];

// handle requests for updating the UI
var socket = io.connect('http://localhost:3000');

socket.on('add', function (data) {
	  $("#currentlyDrivingTaxis").text(markers.length);
	  
    createOrMoveMarker(map, data.taxiId, data.latitude, data.longitude);
});

socket.on('stats', function (data) {
    $("#currentlyDrivingTaxis").text(data.taxiCount);
    $("#overallDistance").text(parseFloat(data.distance).toFixed(2) + ' km');
});

socket.on('violation', function (data) {
    violations[data.taxiId] = data.distance;
    if (data.distance >= 15.00) {
        removeTaxi(data.taxiId);
        reloadIncidentList();
    }
    reloadViolationsList();
});

socket.on('stop', function (data) {
    removeTaxi(data.taxiId);
    reloadIncidentList();
    reloadViolationsList();
});

socket.on('incident', function (data) {
    incidents[data.taxiId] = data.speed;
    reloadIncidentList();
});
// google maps
function initMap() {
    var forbiddenCity = new google.maps.LatLng(39.916320, 116.397155);
    var mapCanvas = document.getElementById("map");
    var mapOptions = {
        center: forbiddenCity,
        zoom: 11
    };
    map = new google.maps.Map(mapCanvas, mapOptions);

    // taxi area
    var cityCircle = new google.maps.Circle({
        strokeColor: '#FF0000',
        map: map,
        center: forbiddenCity,
        radius: 10000
    });

};

function createOrMoveMarker(map, taxiId, lat, lng) {
    if (markers[taxiId] != undefined) {
        markers[taxiId].setPosition(new google.maps.LatLng(lat, lng));
        return;
    }
    var icon = {
        url: "img/car.png",
        scaledSize: new google.maps.Size(20, 20), // scaled size
        origin: new google.maps.Point(0, 0),
        anchor: new google.maps.Point(0, 0)
    }
    var marker = new google.maps.Marker({position: new google.maps.LatLng(lat, lng), map: map, icon: icon});
    markers[taxiId] = marker;
};
function reloadViolationsList() {
    $("#areaViolations").empty();
    for (var taxiId in violations) {
        var distance = parseFloat(violations[taxiId]).toFixed(2);
        $("#areaViolations").append('<li>Taxi ' + taxiId + ' (' + distance + ' km)' + '</li>');
    }
}

function reloadIncidentList() {
    $("#speedingIncidents").empty();
    for (var taxiId in incidents) {
        var speed = parseFloat(incidents[taxiId]).toFixed(2)
        $("#speedingIncidents").append('<li>Taxi ' + taxiId + ' (' + speed + ' km/h)' + '</li>');
    }
}
function removeTaxi(taxiId) {
    if (markers[taxiId] != undefined) {
        markers[taxiId].setMap(null);
        //Remove all taxi references
        delete markers[taxiId];
        delete violations[taxiId];
        delete incidents[taxiId];
    }

};

