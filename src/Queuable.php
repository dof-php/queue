<?php

declare(strict_types=1);

namespace DOF\Queue;

use DOF\Util\Task;

interface Queuable
{
    public function enqueue(string $queue, Task $job);

    public function dequeue(string $queue) :? Task;

    public function restart(string $queue);

    public function setRestart(string $queue) : bool;

    public function needRestart(string $queue) : bool;
}
