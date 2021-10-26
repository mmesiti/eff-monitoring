<?php

//*****************************************************************************
//*** Ed Bennett
//*** Supercomputing Wales
//*****************************************************************************


ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);


function slurm_interval_to_seconds($interval)
{
    preg_match(
        '/(?:([0-9]+)-)?([0-9][0-9])?:?([0-9][0-9]):([0-9][0-9](?:[.][0-9]+)?)/',
	$interval,
	$matches
    );
    if (isset($matches[0])) {
        $result = (int)round((float)$matches[4], 0);
        $result += 60 * intval($matches[3]);
        $result += 60 * 60 * intval($matches[2]);
        $result += 24 * 60 * 60 * intval($matches[1]);
        return $result;
    } else {
        return 0;
    }
}

function replace_element(&$fields, $from, $to) {
    if (in_array($from, $fields)) {
        $index = array_search($from, $fields);
        $fields[$index] = $to;
    }
}

require_once('functions.php');

// mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

$db = null;
$db = GrafanaConnect();

if (isset($argv[1])) {
    $_FILES = ['sacct' => ['tmp_name' => $argv[1]]];
}

if (isset($_FILES['sacct'])) {
    // Fields to be included
    $included_fields = [
        "Account", "AdminComment", "AllocCPUS", "AllocGRES", "AllocNodes",
        "AllocTRES", "AssocID", "Cluster", "CPUTime", "DerivedExitCode",
        "Elapsed", "Eligible", "End", "ExitCode", "GID", "UnixGroup",
        "JobID", "JobName", "NCPUS", "NNodes", "NodeList", "Priority",
        "SlurmPartition", "QOS", "QOSRAW", "ReqCPUFreq", "ReqCPUFreqMin",
        "ReqCPUFreqMax", "ReqCPUFreqGov", "ReqCPUS", "ReqGRES", "ReqMem",
        "ReqNodes", "ReqTRES", "Reservation", "ReservationId",
        "Reserved", "ResvCPU", "Start", "State", "Submit", "Suspended",
        "SystemCPU", "Timelimit", "TotalCPU", "UID", "User", "UserCPU",
        "WCKeyID", "ReqMemPer"
    ];
    $types = "ssisisisssssssisisiisississssississssssssssssissis";

    // Field data types
    $integer_mangle_fields = ["ReqMem"];
    $integer_nullable_fields = [
        "ReqCPUFreq", "ReqCPUFreqMin", "ReqCPUFreqMax", "ReqCPUFreqGov",
        "ResvCPU", "SystemCPU", "UserCPU"
    ];

    // Leave long integers as strings because PHP
    $integer_fields = [
        "AllocCPUS", "AllocNodes", "AssocID", "GID", "JobID", "NCPUS",
        "NNodes", "Priority", "QOSRAW", "ReqCPUS", "ReqNodes",
        "ReservationId", "Suspended", "UID", "WCKeyID"
    ];

    date_default_timezone_set('UTC');

    // Fields to be converted to numbers of seconds
    $time_interval_fields = [
        "CPUTime", "Elapsed", "Reserved", "ResvCPU", "Suspended",
        "SystemCPU", "Timelimit", "TotalCPU", "UserCPU"
    ];

    // Jobs in states not listed here will be discarded
    $interesting_states = [
        "BOOT_FA", "CANCELL", "COMPLET", "DEADLI", "FAILED",
        "NODE_FA", "PREEMPT", "SUSPEND", "TIMEOU"
    ];

    // Prepare SQL statement
    $fieldlist = implode(',',$included_fields);
    $qs = str_repeat("?,",count($included_fields)-1);
    $update_clause = "ON DUPLICATE KEY UPDATE";
    foreach($included_fields as $field) {
        $update_clause = $update_clause . " $field = VALUES($field),";
    }
    $update_clause = substr($update_clause, 0, -1);
    $sql = "INSERT INTO sacct($fieldlist) values(${qs}?) "
         . $update_clause;
    $query = $db->prepare($sql);

    // Get the file and its header
    $file_name = $_FILES['sacct']['tmp_name'];
    if (!$file_name) {
        printf("File dropped during transit. Try reducing the filesize.\n");
        exit;
    }
    $csv_file = fopen($file_name, "r");
    $fields = fgetcsv($csv_file, 0, "|");
    $line_number = 1;
    $name_index = array_search("JobName", $fields);
    $name_reverse_index = $name_index - count($fields);

    // Rename fields that are SQL reserved words
    replace_element($fields, "Group", "UnixGroup");
    replace_element($fields, "Partition", "SlurmPartition");

    // Swap in the raw JobID to avoid discarding array job steps
    replace_element($fields, "JobID", "JobID_userfacing");
    replace_element($fields, "JobIDRaw", "JobID");

    while (($line = fgetcsv($csv_file, 0, "|")) !== FALSE) {
        $line_number += 1;
       	if (count($line) > count($fields)) {
            $before_name = array_slice($line, 0, $name_index);
            $after_name = array_slice($line, $name_reverse_index + 1);
            $name_array = array_slice($line, $name_index,
                                      count($line) - count($fields) + 1);
            $job_name = implode("|", $name_array);
            $line = array_merge($before_name, [$job_name], $after_name);
        } elseif (count($line) < count($fields)) {
            print("Not enough fields on line $line_number.\n");
            continue;
        }
        $record = array_combine($fields, $line);

        // Discard uninteresting states
        if (!in_array(substr($record['State'], 0, 7), $interesting_states)) {
            continue;
        }

        // Discard sub-jobs
        if (preg_match('/[^0-9]/', $record['JobID'])) {
            continue;
        }

        // Discard unused fields for whole jobs
        foreach ($fields as $field) {
            if (!in_array($field, $included_fields)) {
                unset($record[$field]);
            }
        }

        // Convert time intervals to be numbers of seconds
        foreach ($time_interval_fields as $field) {
            $record[$field] = slurm_interval_to_seconds($record[$field]);
        }

        // Mangle ReqMem
        if (in_array("ReqMem", $fields)) {
            $record["ReqMemPer"] = substr($record["ReqMem"], -1);
            $record["ReqMem"] = substr($record["ReqMem"], 0, -1);
        }

        // Mangle integers with suffixes
        foreach ($integer_mangle_fields as $field) {
            $result = intval(substr($record[$field], 0, -1));
            switch (substr($record[$field], -1)) {
            case 'k':
                $result *= 1000;
                break;
            case 'M':
                $result *= 1000000;
                break;
            case 'G':
                $result *= 1000000000;
                break;
            case 'T':
                $result *= 1000000000000;
                break;
            default:
                $result = intval($record[$field]);
            }
            $record[$field] = $result;
        }

        // Cast nullable integers
        foreach ($integer_nullable_fields as $field) {
            if (is_numeric($record[$field])) {
                $record[$field] = intval($record[$field]);
            } else {
                $record[$field] = NULL;
            }
        }

        // Cast other integers
        foreach ($integer_fields as $field) {
            $record[$field] = intval($record[$field]);
        }

        // Turn the array into an array of references
        // because mysqli doesn't like it otherwise
        $record_refs = [];
        foreach($record as $key => $value) {
            $record_refs[$key] = &$record[$key];
        }
        call_user_func_array(
            [$query, "bind_param"],
            array_merge([$types], array_values($record_refs))
        );
        $query_result = $query->execute();
        if(!$query_result) {
            printf("%s\n", mysqli_error($db));
        } else {
            print("Successfully added job " . $record["JobID"] . ".\n");
        }
    }
} else {
    echo "No files found. Try reducing the file size.\n";
}

GrafanaDisconnect($db);

?>
