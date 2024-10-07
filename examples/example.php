<?php

require_once __DIR__ . '/../vendor/autoload.php';

use ReactphpX\MySQL\Pool;
use React\MySQL\MysqlResult;
use React\EventLoop\Loop;

$pool = new Pool(getenv('MYSQL_URI') ?: 'username:password@host/databasename?timeout=5');

// query($pool);
queryStream($pool);

function query($pool)
{
    for ($i = 0; $i < 120; $i++) {
        $pool->query('select * from blog')->then(function (MysqlResult $command) use ($i) {
            echo "query:$i\n";
            if (isset($command->resultRows)) {
                // this is a response to a SELECT etc. with some rows (0+)
                // print_r($command->resultFields);
                // print_r($command->resultRows);
                echo count($command->resultRows) . ' row(s) in set' . PHP_EOL;
            } else {
                // this is an OK message in response to an UPDATE etc.
                if ($command->insertId !== 0) {
                    var_dump('last insert ID', $command->insertId);
                }
                echo 'Query OK, ' . $command->affectedRows . ' row(s) affected' . PHP_EOL;
            }
        }, function (\Exception $error) {
            // the query was not executed successfully
            echo 'Error: ' . $error->getMessage() . PHP_EOL;
        });
    }
}

function queryStream($pool)
{
    for ($i = 0; $i < 120; $i++) {
        (function ($pool, $i) {
            $stream = $pool->queryStream('select * from blog');

            $stream->on('data', function ($data) use ($i) {
                // echo "queryStream:$i\n";
                // print_r($data);
            });
            $stream->on('error', function ($err) use ($i) {
                echo 'Error: ' .$i.' ' .$err->getMessage() . PHP_EOL;
            });
            $stream->on('end', function () use ($i) {
                echo 'Completed.' . $i . PHP_EOL;
            });
        })($pool, $i);
    }
}

\React\EventLoop\Loop::addTimer(10, function () use ($pool) {
    // queryStream($pool);
});


// Loop::addPeriodicTimer(2, function () use ($pool) {
//     // query($pool);
//     // queryStream($pool);
//     echo 'pool_count:' . $pool->getPoolCount() . PHP_EOL;
//     echo 'idleConnectionCount:' . $pool->idleConnectionCount() . PHP_EOL;
// });

$pool->transaction(function ($connection) {
    // throw new Exception("Error Processing Request", 1);

    return \React\Async\await($connection->query("INSERT INTO blog_test (content) VALUES ('hello world success')"));
})->then(function ($result) {
    var_dump($result);
}, function ($error) {
    var_dump($error->getMessage());
});
