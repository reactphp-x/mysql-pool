<?php

namespace ReactphpX\MySQL;

use React\MySQL\Factory;
use React\EventLoop\LoopInterface;
use React\Socket\ConnectorInterface;
use React\Promise\Deferred;
use React\MySQL\ConnectionInterface;
use React\MySQL\QueryResult;
use React\MySQL\Exception;
use React\EventLoop\Loop;
use ReactphpX\Pool\AbstractConnectionPool;
use function React\Promise\resolve;

class Pool extends AbstractConnectionPool implements ConnectionInterface,TranactionInterface
{
    use \Evenement\EventEmitterTrait;

    private $factory;
    private $uri;

    public function __construct(
        $uri,
        $config = [],
        Factory $factory = null,
        LoopInterface $loop = null,
        ConnectorInterface $connector = null
    ) {
        $this->uri = $uri;
        $this->factory = $factory ?: new Factory($loop, $connector);;
        parent::__construct($config, $loop);
    }

    public function query($sql, array $params = [])
    {
        $deferred = new Deferred();

        $this->getConnection()->then(function (ConnectionInterface $connection) use ($sql, $params, $deferred) {
            $connection->query($sql, $params)->then(function (QueryResult $command) use ($deferred, $connection) {
                $this->releaseConnection($connection);
                try {
                    $deferred->resolve($command);
                } catch (\Throwable $th) {
                    //todo handle $th
                }
            }, function ($e) use ($deferred, $connection) {
                $deferred->reject($e);

                $this->_ping($connection);
            });
        }, function ($e) use ($deferred) {
            $deferred->reject($e);
        });

        return $deferred->promise();
    }
    
    public function queryStream($sql, $params = [])
    {
        $error = null;

        $stream = \React\Promise\Stream\unwrapReadable(
            $this->getConnection()->then(function (ConnectionInterface $connection) use ($sql, $params) {
                $stream = $connection->queryStream($sql, $params);
                $stream->on('end', function () use ($connection) {
                    $this->releaseConnection($connection);
                });
                $stream->on('error', function ($err) use ($connection) {
                    $this->_ping($connection);
                });
                return $stream;
            }, function ($e) use (&$error) {
                $error = $e;
                throw $e;
            })
        );

        if ($error) {
            Loop::addTimer(0.0001, function () use ($stream, $error) {
                $stream->emit('error', [$error]);
            });
        }

        return $stream;
    }

    public function ping()
    {
        throw new \Exception("not support");
    }

    public function quit()
    {
        $this->close();
        return resolve(true);
    }


    public function transaction(callable $callable)
    {
        $that = $this;
        $deferred = new Deferred();

        $this->getConnection()->then(function (ConnectionInterface $connection) use ($callable, $deferred, $that) {
            $connection->query('BEGIN')
                ->then(function () use ($callable, $connection) {
                    try {
                        return \React\Async\async(function () use ($callable, $connection) {
                            return $callable($connection);
                        })();
                    } catch (\Throwable $th) {
                        throw $th;
                    }
                })
                ->then(function ($result) use ($connection, $deferred, $that) {
                    $connection->query('COMMIT')->then(function () use ($result, $deferred, $connection, $that) {
                        $that->releaseConnection($connection);
                        $deferred->resolve($result);
                    }, function ($error) use ($deferred, $connection, $that) {
                        $that->_ping($connection);
                        $deferred->reject($error);
                    });
                }, function ($error) use ($connection, $deferred, $that) {
                    $connection->query('ROLLBACK')->then(function () use ($error, $deferred, $connection, $that) {
                        $that->releaseConnection($connection);
                        $deferred->reject($error);
                    }, function () use ($deferred, $connection, $that, $error) {
                        $that->_ping($connection);
                        $deferred->reject($error);
                    });
                });
        }, function ($error) use ($deferred) {
            $deferred->reject($error);
        });

        return $deferred->promise();
    }

    protected function createConnection()
    {
        return $this->factory->createLazyConnection($this->uri);
    }
}
