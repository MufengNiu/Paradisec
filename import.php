<?php

require 'vendor/autoload.php';

use Carbon\Carbon;

//Database connection parameters
$dbHost = '127.0.0.1';
$dbPort = '5432';
$dbName = 'tlcmap';
$dbUsername = 'postgres';
$dbPassword = 'root';

//The nominated owner id
$ownerId = 103;

if ($argc == 2) {
    $command = $argv[1];

    if ($command === 'import') {
        import($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword, $ownerId);
        echo "Import Successfully\n";
    } else if ($command === 'undo') {
        undo($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword);
        echo "Undo import successfully\n";
    } else if ($command === 'update') {
        update($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword, $ownerId);
        echo "Update successfully\n";
    } else {
        echo "Invalid command\n";
    }
} else {
    echo "No command specified\n";
}


//import from paradisec
function import($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword, $ownerId)
{

    $conn = getDbConnection($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword);

    $datasetIdMapping = [];
    $placeIdMapping = [];

    //Main collection
    $mainCollectionUrl = 'https://catalog.paradisec.org.au/collections.geo_json';
    $mainCollectionData = fetchGeoJSON($mainCollectionUrl);
    addDatasetAndPlaces($conn, $mainCollectionData, $ownerId, "PARADISEC collections", false, false, $datasetIdMapping, $placeIdMapping);

    //Sub collection
    foreach ($mainCollectionData['features'] as $feature) {
        $subCollectionUrl = "https://catalog.paradisec.org.au/collections/" . $feature['properties']['id'] . ".geo_json";
        $subCollectionData = fetchGeoJSON($subCollectionUrl);
        addDatasetAndPlaces($conn, $subCollectionData, $ownerId, null, true, true, $datasetIdMapping, $placeIdMapping);
    }

    pg_close($conn);

    $folder = 'mapping';
    $filename = $folder . '/dataset_mapping.json';

    // Write the datasetIds array to a JSON file
    file_put_contents($filename, json_encode($datasetIdMapping, JSON_PRETTY_PRINT));

    // Write the placeIds array to a JSON file
    $filename = $folder . '/place_mapping.json';
    file_put_contents($filename, json_encode($placeIdMapping, JSON_PRETTY_PRINT));
}


function getDbConnection($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword)
{

    $conn = pg_connect("host=$dbHost port=$dbPort dbname=$dbName user=$dbUsername password=$dbPassword");
    if (!$conn) {
        die("Connection failed: " . pg_last_error());
    }

    return $conn;
}

function fetchGeoJSON($url) {
    $response = file_get_contents($url);
    if ($response === false) {
        die("Failed to fetch the URL");
    }
    return json_decode($response, true);
}

//  1. create dataset
//  2. Add relationship to user_dataset table
//  3. Add places
//  Mapping:
//        name: "PARADISEC collections" for the main collection, {name} for the sub-collection
//        description: from the description of the FeatureCollection metadata
//        public: true
//        publisher: from the publisher of the FeatureCollection metadata
//        contact: from the contact of the FeatureCollection metadata
//        source_url: from the url of the FeatureCollection metadata
//        license: main collcetion: null ;  sub-collection:  from the license of the FeatureCollection metadata
//        rights:  main collcetion: null ;  sub-collection:  from the rights of the FeatureCollection metadata
//        recordtype_id: 1 (Other)
//        linkback: the same as source_url
//        created_at: Current time
//        updated_at: Current time
function addDatasetAndPlaces($conn, $data, $ownerId, $datasetName = null, $setLicense = false, $setRights = false, &$datasetIdMapping, &$placeIdMapping)
{
    $metadata = $data['metadata'];

    $createdAt = Carbon::now();
    $updatedAt = Carbon::now();

    $sql = "INSERT INTO tlcmap.dataset (name, description, public, publisher, contact, source_url, license , rights , recordtype_id, linkback, created_at, updated_at) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 , $11 , $12)
            RETURNING id";

    $result = pg_query_params($conn, $sql, [
        isset($datasetName) ? $datasetName : (isset($metadata['name']) ? $metadata['name'] : 'Unknown'),
        isset($metadata['description']) ? $metadata['description'] : ' ',
        true,
        $metadata['publisher'],
        $metadata['contact'],
        $metadata['url'],
        $setLicense ? $metadata['license'] : null,
        $setRights ? $metadata['rights'] : null,
        1,
        $metadata['url'],
        $createdAt,
        $updatedAt
    ]);

    if (!$result) {
        die("Layer creation failed: " . pg_last_error($conn));
    }

    $row = pg_fetch_assoc($result);
    $layerId = $row['id'];

    echo "Layer created with ID: $layerId\n";

    // Add ownership
    addOwnership($conn, $ownerId, $layerId);
    $datasetIdMapping[][$metadata['id']] = $layerId;

    // Add places
    addPlaces($conn, $data['features'], $layerId, $placeIdMapping);
}


// Add relationship to user_dataset table
// Mapping: 
//         user_id: The user ID of the nominated owner.
//         dataset_id: The ID of the layer just created.
//         dsrole: "OWNER"
//         created_at: Current time
//         updated_at: Current time
function addOwnership($conn, $ownerId, $layerId)
{
    $createdAt = Carbon::now()->toDateTimeString();
    $updatedAt = Carbon::now()->toDateTimeString();

    $sql = "INSERT INTO tlcmap.user_dataset (user_id, dataset_id, dsrole, created_at, updated_at) 
            VALUES ($1, $2, $3, $4, $5)";

    $result = pg_query_params($conn, $sql, [
        $ownerId,
        $layerId,
        'OWNER',
        $createdAt,
        $updatedAt
    ]);

    if (!$result) {
        die("user_dataset table update failed: " . pg_last_error($conn));
    }
}


// Add places (dataitems) to datasets
// Mapping:
//        dataset_id: The ID of the layer just created.
//        title: feature property name
//        recordtype_id: 1 (Other)
//        description: feature property description
//        latitude: from feature coordinates
//        longitude: from feature coordinates
//        datestart: convert feature property udatestart to date string "yyyy-mm-dd"
//        dateend: same as datestart
//        source: feature property url
//        external_url: feature property url
//        extended_data: constructed XML
//        datasource_id: 1 (GHAP)
//        created_at: Current time
//        updated_at: Current time
function addPlaces($conn, $features, $datasetId, &$placeIdMapping)
{
    if (!isset($features) || !is_array($features) || count($features) == 0) {
        return;
    }

    foreach ($features as $feature) {
        $properties = $feature['properties'];

        $coordinates = $feature['geometry']['coordinates'];
        $latitude = null;
        $longitude = null;
        if (isset($coordinates) && is_array($coordinates) && count($coordinates) == 2) {
            $latitude = $coordinates[1];
            $longitude = $coordinates[0];
        }
        $extendedData = generatePlaceExtendedData($properties);

        $createdAt = Carbon::now();
        $updatedAt = Carbon::now();


        $sql = "INSERT INTO tlcmap.dataitem (dataset_id, title, recordtype_id, description, latitude, longitude, datestart, dateend, source, external_url, extended_data, datasource_id, created_at, updated_at) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id";

        $result = pg_query_params($conn, $sql, [
            $datasetId,
            $properties['name'],
            1,
            $properties['description'],
            $latitude,
            $longitude,
            getDateByTimestamp($properties['udatestart']),
            getDateByTimestamp($properties['udatestart']),
            $properties['url'],
            $properties['url'],
            $extendedData,
            1,
            $createdAt,
            $updatedAt
        ]);

        if (!$result) {
            die("Place creation failed: " . pg_last_error($conn));
        }

        $row = pg_fetch_assoc($result);
        $placeId = $row['id'];
        $uid = createPlaceUid($placeId, 't');

        // Update the uid for the place record
        $updateSql = "UPDATE tlcmap.dataitem SET uid = $1 WHERE id = $2";
        $updateResult = pg_query_params($conn, $updateSql, [
            $uid,
            $placeId
        ]);

        if (!$updateResult) {
            die("place UID update failed: " . pg_last_error($conn));
        }

        $placeIdMapping[][$properties['id']] = $placeId;

        //echo "Place created with ID: $placeId and UID: $uid\n";
    }
}

// Construct extended_data string for place
//      Mapping:
//               ID: feature property id
//               Languages: feature property languages
//               Countries: feature property countries
//               Publisher: feature property publisher
//               Contact: feature property contact
//               License: feature property license
//               Rights: feature property rights
function generatePlaceExtendedData($properties)
{

    $extdata = '';

    if (isset($properties['id']) && $properties['id'] !== '') {
        $extdata = $extdata . '<Data name="ID"><value><![CDATA[' . trim($properties['id']) . ']]></value></Data>';
    }
    if (isset($properties['languages']) && $properties['languages'] !== '') {
        $extdata = $extdata . '<Data name="Languages"><value><![CDATA[' . trim($properties['languages']) . ']]></value></Data>';
    }
    if (isset($properties['countries']) && $properties['countries'] !== '') {
        $extdata = $extdata . '<Data name="Countries"><value><![CDATA[' . trim($properties['countries']) . ']]></value></Data>';
    }
    if (isset($properties['publisher']) && $properties['publisher'] !== '') {
        $extdata = $extdata . '<Data name="Publisher"><value><![CDATA[' . trim($properties['publisher']) . ']]></value></Data>';
    }
    if (isset($properties['contact']) && $properties['contact'] !== '') {
        $extdata = $extdata . '<Data name="Contact"><value><![CDATA[' . trim($properties['contact']) . ']]></value></Data>';
    }
    if (isset($properties['license']) && $properties['license'] !== '') {
        $extdata = $extdata . '<Data name="License"><value><![CDATA[' . trim($properties['license']) . ']]></value></Data>';
    }
    if (isset($properties['rights']) && $properties['rights'] !== '') {
        $extdata = $extdata . '<Data name="Rights"><value><![CDATA[' . trim($properties['rights']) . ']]></value></Data>';
    }

    if (!empty($extdata)) {
        $extendedData = '<ExtendedData>' . $extdata . '</ExtendedData>';
    } else {
        $extendedData["extended_data"] = null;
    }

    return $extendedData;
}

// Create uid for place
function createPlaceUid($placeId, $prefix = 't')
{
    if (!empty($placeId)) {
        return $prefix . base_convert($placeId, 10, 16);
    }
    return null;
}


function getDateByTimestamp($timestamp)
{
    if (!isset($timestamp)) {
        return null;
    }

    // If the timestamp is in milliseconds, convert it to seconds
    if (abs($timestamp) > 10000000000) {
        $timestamp = $timestamp / 1000;
    }


    return date('Y-m-d', $timestamp);
}

//Delete all import
function undo($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword)
{
    $conn = getDbConnection($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword);

    $folder = 'mapping';
    $filename = $folder . '/dataset_mapping.json';
    $filename2 = $folder . '/place_mapping.json';

    if (!file_exists($filename) || !file_exists($filename2)) {
        die("JSON files does not exist.\n");
    }

    $jsonContent = file_get_contents($filename);
    $datasetMapping = json_decode($jsonContent, true);
    $datasetIds = array_merge(...array_map('array_values', $datasetMapping));

    foreach ($datasetIds as $datasetId) {

        //Delete datasets
        $sql = "DELETE FROM tlcmap.dataset WHERE id = $1";
        $result = pg_query_params($conn, $sql, [
            $datasetId
        ]);
        if (!$result) {
            die("Dataset deletion failed: " . pg_last_error($conn));
        }

        //Delete ownership
        $sql = "DELETE FROM tlcmap.user_dataset WHERE dataset_id = $1";
        $result = pg_query_params($conn, $sql, [
            $datasetId
        ]);
        if (!$result) {
            die("Ownership deletion failed: " . pg_last_error($conn));
        }

        //Delete places
        $sql = "DELETE FROM tlcmap.dataitem WHERE dataset_id = $1";
        $result = pg_query_params($conn, $sql, [$datasetId]);
        if (!$result) {
            die("Places deletion failed: " . pg_last_error($conn));
        }
    }

    //Delete file
    unlink($filename);
    unlink($filename2);
    pg_close($conn);
}

// Update all import
function update($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword, $ownerId)
{
    $conn = getDbConnection($dbHost, $dbPort, $dbName, $dbUsername, $dbPassword);

    // Read the mapping from file
    $folder = 'mapping';
    $filename1 = $folder . '/dataset_mapping.json';
    if (!file_exists($filename1)) {
        die("Mapping JSON file does not exist.\n");
    }
    $jsonContent = file_get_contents($filename1);
    $datasetIdMappingArray = json_decode($jsonContent, true);
    $datasetIdMapping = [];
    foreach ($datasetIdMappingArray as $mapping) {
        foreach ($mapping as $key => $value) {
            $datasetIdMapping[$key] = $value;
        }
    }

    $filename2 = $folder . '/place_mapping.json';
    if (!file_exists($filename2)) {
        die("Mapping JSON file does not exist.\n");
    }
    $jsonContent = file_get_contents($filename2);
    $placeIdMappingArray = json_decode($jsonContent, true);
    $placeIdMapping = [];
    foreach ($placeIdMappingArray as $mapping) {
        foreach ($mapping as $key => $value) {
            $placeIdMapping[$key] = $value;
        }
    }

    $mainCollectionUrl = 'https://catalog.paradisec.org.au/collections.geo_json';
    $mainCollectionData = fetchGeoJSON($mainCollectionUrl);

    // UPDATE main collection
    updateDatasetAndPlaces($conn, $mainCollectionData, $datasetIdMapping["PARADISEC"], "PARADISEC collections", false, false, $placeIdMapping);

    foreach ($mainCollectionData['features'] as $feature) {
        $featureId = $feature['properties']['id'];
        $subCollectionUrl = "https://catalog.paradisec.org.au/collections/" . $featureId . ".geo_json";
        $subCollectionData = fetchGeoJSON($subCollectionUrl);

        if (isset($datasetIdMapping[$featureId])) {
            //Update
            $layerId = $datasetIdMapping[$featureId];
            updateDatasetAndPlaces($conn, $subCollectionData, $layerId, null, true, true, $placeIdMapping);
        } else {
            // Add New places
            addDatasetAndPlaces($conn, $subCollectionData, $ownerId, null, true, true, $datasetIdMapping, $placeIdMapping);
        }
    }

    pg_close($conn);
    file_put_contents($filename1, json_encode($datasetIdMapping, JSON_PRETTY_PRINT));
    file_put_contents($filename2, json_encode($placeIdMapping, JSON_PRETTY_PRINT));
}

// Update dataset and places
function updateDatasetAndPlaces($conn, $data, $datasetID, $datasetName = null, $setLicense = false, $setRights = false, $placeIdMapping)
{

    $metadata = $data['metadata'];
    $updatedAt = Carbon::now();

    $sql = "UPDATE tlcmap.dataset 
    SET name = $1, description = $2, public = $3, publisher = $4, contact = $5, source_url = $6, license = $7,
        rights = $8, recordtype_id = $9, linkback = $10, updated_at = $11
    WHERE id = $12";

    $result = pg_query_params($conn, $sql, [
        isset($datasetName) ? $datasetName : (isset($metadata['name']) ? $metadata['name'] : 'Unknown'),
        isset($metadata['description']) ? $metadata['description'] : ' ',
        true,
        $metadata['publisher'],
        $metadata['contact'],
        $metadata['url'],
        $setLicense ? $metadata['license'] : null,
        $setRights ? $metadata['rights'] : null,
        1,
        $metadata['url'],
        $updatedAt,
        $datasetID
    ]);

    if (!$result) {
        die("Layer updation failed: " . pg_last_error($conn));
    }

    echo "Layer updated with ID: $datasetID\n";
    updatePlaces($conn, $data['features'], $datasetID, $placeIdMapping);
}


// Update places
function updatePlaces($conn, $features, $datasetId, &$placeIdMapping)
{
    if (!isset($features) || !is_array($features) || count($features) == 0) {
        return;
    }

    foreach ($features as $feature) {

        $properties = $feature['properties'];
        $featureId = $properties['id'];

        $coordinates = $feature['geometry']['coordinates'];
        $latitude = null;
        $longitude = null;
        if (isset($coordinates) && is_array($coordinates) && count($coordinates) == 2) {
            $latitude = $coordinates[1];
            $longitude = $coordinates[0];
        }
        $extendedData = generatePlaceExtendedData($properties);

        $createdAt = Carbon::now();
        $updatedAt = Carbon::now();

        if (isset($placeIdMapping[$featureId])) {
            //Update
            $placeId = $placeIdMapping[$featureId];
            $sql = "UPDATE tlcmap.dataitem 
            SET dataset_id = $1, title = $2, recordtype_id = $3, description = $4, latitude = $5, longitude = $6, 
                datestart = $7, dateend = $8, source = $9, external_url = $10, extended_data = $11, datasource_id = $12, 
                updated_at = $13 
            WHERE id = $14";

            $result = pg_query_params($conn, $sql, [
                $datasetId,
                $properties['name'],
                1,
                $properties['description'],
                $latitude,
                $longitude,
                getDateByTimestamp($properties['udatestart']),
                getDateByTimestamp($properties['udatestart']),
                $properties['url'],
                $properties['url'],
                $extendedData,
                1,
                $updatedAt,
                $placeId
            ]);

            if (!$result) {
                die("Place updation failed: " . pg_last_error($conn));
            }
            // echo "Place updated with ID: " . $placeIdMapping[$properties['id']] . " \n";
        } else {
            //Add

            $sql = "INSERT INTO tlcmap.dataitem (dataset_id, title, recordtype_id, description, latitude, longitude, datestart, dateend, source, external_url, extended_data, datasource_id, created_at, updated_at) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10 , $11 , $12 , $13 , $14)
                    RETURNING id";

            $result = pg_query_params($conn, $sql, [
                $datasetId,
                $properties['name'],
                1,
                $properties['description'],
                $latitude,
                $longitude,
                getDateByTimestamp($properties['udatestart']),
                getDateByTimestamp($properties['udatestart']),
                $properties['url'],
                $properties['url'],
                $extendedData,
                1,
                $createdAt,
                $updatedAt
            ]);

            if (!$result) {
                die("Place creation failed: " . pg_last_error($conn));
            }

            $row = pg_fetch_assoc($result);
            $placeId = $row['id'];
            $uid = createPlaceUid($placeId, 't');

            // Update the uid for the place record
            $updateSql = "UPDATE tlcmap.dataitem SET uid = $1 WHERE id = $2";
            $updateResult = pg_query_params($conn, $updateSql, [
                $uid,
                $placeId
            ]);

            if (!$updateResult) {
                die("place UID update failed: " . pg_last_error($conn));
            }

            $placeIdMapping[][$properties['id']] = $placeId;

            //echo "Place created with ID: $placeId and UID: $uid\n";
        }
    }
}
