<?php

declare(strict_types=1);

namespace DOF\Queue;

use DOF\Util\Task;
use DOF\Queue\Queue;

class Redis extends Queue
{
    public function enqueue(string $queue, Task $task)
    {
        $start = \microtime(true);

        $job = \serialize($task);

        $this->connection()->rPush($this::name($queue, Queue::NORMAL), $job);

        $this->log('enqueue', $start, $queue, $job);
    }

    public function dequeue(string $queue) :? Task
    {
        $start = \microtime(true);

        $task = $this->connection()->lPop($this::name($queue, Queue::NORMAL));

        $this->log('dequeue', $start, $queue);

        return $task ? \unserialize($task) : null;
    }

    public function setRestart(string $queue) : bool
    {
        $start = \microtime(true);

        $res = $this->connection()->set($this::name($queue, Queue::RESTART), $start);

        $this->log('setRestart', $start, $queue, $start);

        return $res === true;
    }

    public function restart(string $queue)
    {
        $start = \microtime(true);

        $res = $this->connection()->del($this::name($queue, Queue::RESTART));

        $this->log('restart', $start, $queue);

        return $res;
    }

    public function needRestart(string $queue) : bool
    {
        $start = \microtime(true);

        $res = $this->connection()->get($this::name($queue, Queue::RESTART));

        $this->log('needRestart', $start, $queue);

        return false !== $res;
    }
}
