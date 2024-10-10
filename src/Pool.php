<?php

namespace ReactphpX\MySQL;


use React\Mysql\MysqlClient;
use React\EventLoop\Loop;
use ReactphpX\Pool\AbstractConnectionPool;

final class Pool extends AbstractConnectionPool implements TranactionInterface
{
    protected function createConnection()
    {
        // $connection = new MysqlClient($this->uri);
        $connection = (new \React\MySQL\Factory())->createLazyConnection($this->uri);
        $connection->on('close', function () use ($connection) {
            if ($this->pool->contains($connection)) {
                $this->pool->detach($connection);
            }
            $this->currentConnections--;
        });
        $this->currentConnections++;
        return $connection;
    }

    public function query(string $sql, array $params = [])
    {
        return $this->getConnection()->then(function ($connection) use ($sql, $params) {
            return $connection->query($sql, $params)->then(function ($result) use ($connection) {
                $this->releaseConnection($connection);
                return $result;
            }, function ($error) use ($connection) {
                $this->releaseConnection($connection);
                throw $error;
            });
        });
    }

    public function queryStream(string $sql, array $params = [])
    {
        $error = null;
        $p = $this->getConnection()->then(function ($connection) use ($sql, $params) {
            $stream = $connection->queryStream($sql, $params);
            $stream->on('close', function () use ($connection) {
                $this->releaseConnection($connection);
            });
            return $stream;
        })->then(null, function ($e) use (&$error) {
            $error = $e;
        });

        if ($error) {
            $stream = new \React\Stream\ThroughStream();
            Loop::futureTick(function () use ($stream, $error) {
                $stream->emit('error', [$error, $stream]);
                $stream->close();
            });
            return $stream;
        }

        return \React\Promise\Stream\unwrapReadable($p);
    }
    public function transaction(callable $callable)
    {
        return $this->getConnection()->then(function ($connection) use ($callable) {
            return $connection->query('BEGIN')->then(function () use ($connection, $callable) {
                try {
                    return \React\Async\async(function () use ($callable, $connection) {
                        return $callable($connection);
                    })();
                } catch (\Throwable $th) {
                    throw $th;
                }
            })->then(function ($result) use ($connection) {
                return $connection->query('COMMIT')->then(function () use ($connection, $result) {
                    $this->releaseConnection($connection);
                    return $result;
                });
            }, function ($error) use ($connection) {
                return $connection->query('ROLLBACK')->then(function () use ($connection, $error) {
                    $this->releaseConnection($connection);
                    throw $error;
                });
            });
        });
    }
}
