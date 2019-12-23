<?php

declare(strict_types=1);

namespace DOF\Queue;

use Throwable;
use DOF\Util\Str;
use DOF\Util\Arr;
use DOF\Util\Format;
use DOF\Queue\Driver;
use DOF\Queue\Queuable;
use DOF\Storage\Connection;

abstract class Queue implements Queuable
{
    use \DOF\Storage\Traits\LogableStorage;

    const STORAGE = 'queue';

    const NORMAL  = '__QUEUE:NORMAL';
    const LOCKED  = '__QUEUE:LOCKED';
    const DELAY   = '__QUEUE:DELAY';
    const FAILED  = '__QUEUE:FAILED';
    const TIMEOUT = '__QUEUE:TIMEOUT';
    const RESTART = '__QUEUE:RESTART';

    final public static function name(string $queue, string $prefix = self::NORMAL) : string
    {
        return \join(':', [$prefix, $queue]);
    }
}
