<?php

declare(strict_types=1);

namespace DOF\Queue;

use Closure;
use Throwable;
use DOF\Util\Task;

class Worker
{
    public static function new(Closure $onFailed)
    {
        $pid = pcntl_fork();
        if (-1 === $pid) {
            $onFailed();
            return null;
        }

        return $pid;
    }

    public static function process(Task $job, Closure $onException) : int
    {
        try {
            $job->execute();
        } catch (Throwable $th) {
            $onException($th);

            return -1;
        }

        return 0;
    }
}
