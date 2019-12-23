<?php

declare(strict_types=1);

namespace DOF\Queue;

class Driver extends \DOF\Storage\Driver
{
    use \DOF\Storage\Traits\KVDriver;

    const LIST = [
        'redis' => \DOF\Queue\Redis::class,
    ];

    const KV = 'queue';
}
