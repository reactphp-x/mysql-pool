<?php

namespace Reactphp\Framework\MySQL;

interface TranactionInterface
{
    public function transaction(callable $callable);
   
}