<?php

namespace SawitDB\Laravel;

use Illuminate\Support\ServiceProvider;
use SawitDB\Engine\WowoEngine;

class SawitDBServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->singleton('sawitdb', function ($app) {
            $path = config('database.connections.sawit.database') ?? database_path('sawit.db');
            return new WowoEngine($path);
        });

        $this->app->alias('sawitdb', WowoEngine::class);
    }

    public function boot()
    {
        // Publish config if needed in the future
    }
}
