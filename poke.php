<?php

header('Access-Control-Allow-Origin: https://fastpokemap.se');
header('Content-Type: application/json');

$db = new mysqli("localhost", "pokemongo", "pokemongo", "pokemongo");
if($db->connect_error) {
	die("Connect error: " . $db->connect_errno . " - ". $db->connect_error );
}

$query = 'INSERT INTO spawns(encounter_id, expiration_timestamp, latitude,
					longitude, name, spawn_point_id)
					VALUES ( ?, ?, ?, ?, ?, ? )
					ON DUPLICATE KEY UPDATE name=name';

if( $insert = $db->prepare($query) )
{
	$insert->bind_param( "siddss", $_GET['encounter_id'],  $_GET['expiration_timestamp'], 
		$_GET['latitude'], $_GET['longitude'], $_GET['name'], $_GET['spawn_point_id']);
	$insert->execute();
	
	if($insert->error) {
		die("Insert error: ". $insert->error);
	}
	$insert->close();
}
$db->close();
echo '{"result": []}';
