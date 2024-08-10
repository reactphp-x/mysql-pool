<?php

namespace ReactphpX\MySQL;

interface TranactionInterface
{
    public function transaction(callable $callable);
   
}