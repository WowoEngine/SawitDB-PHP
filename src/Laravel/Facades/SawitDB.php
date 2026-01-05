<?php

namespace SawitDB\Laravel\Facades;

use Illuminate\Support\Facades\Facade;

class SawitDB extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'sawitdb';
    }
}
