<?php

declare(strict_types=1);

namespace DOF\Queue;

use Throwable;
use DOF\Traits\Tracker;
use DOF\Util\Task;
use DOF\Util\JSON;
use DOF\Util\Format;
use DOF\CLI\Console;
use DOF\Queue\Worker;
use DOF\Queue\Queuable;

class Dispatcher
{
    use Tracker;

    private $interval = 3;
    private $timeout = 0;    // 0 means no timeout limit
    private $tryTimes = 0;    // 0 means do not re-execute failed job
    private $tryDelay = -1;    // -1 means do not re-execute failed job
    private $quiet = true;
    private $debug = false;    // Do not fork process as job worker

    /** @var bool: Run queue command in background as daemon process (TODO) */
    private $daemon = false;    // Need supervisor to restart if dispatcher process exit abnormally

    private $console;
    private $queuable;
    private $queue;

    public function looping()
    {
        if ($this->debug) {
            $this->log('RunQueueDispatcherAsJobWorkerInDebugMode');
        } else {
            if (! \extension_loaded('pcntl')) {
                $this->log('PcntlExtensionNotFound');
                return;
            }
        }

        while (true) {
            if ($this->queuable->needRestart($this->queue)) {
                try {
                    $result = $this->queuable->restart($this->queue);
                } catch (Throwable $th) {
                    $this->log('QueueRestartException', Format::throwable($th));
                    continue;
                }

                $this->log('QueueRestarted', \compact('result'));

                // Do not return or exit(0) here
                // Because daemon service like supervisord may not restart if process exit with status code 0
                exit(-1);
            }

            try {
                $job = $this->queuable->dequeue($this->queue);
            } catch (Throwable $th) {
                $this->log('JobDequeueException', Format::throwable($th));
                continue;
            }

            if (! $job) {
                $this->log('NoJobsNow');
                \sleep($this->interval);
                continue;
            }

            if (! ($job instanceof Task)) {
                $this->log('InvalidJob', \compact('job'));
                continue;
            }

            if ($this->debug) {
                $code = Worker::process($job, function ($th) {
                    $this->log('JobExecuteExceptionInDebugMode', Format::throwable($th));
                });
                if (0 === $code) {
                    $this->log('ProcessedSuccessfullyInDebugMode', ['job' => \get_class($job)]);
                }
            } else {
                $worker = Worker::new(function () {
                    $this->log('UnableToForkChildWorker');
                });
                if (\is_null($worker)) {
                    $this->log('FailedToForkChildWorker');
                    continue;
                }

                if ($worker > 0) {
                    // Parent process, waiting for child process
                    // Wait until the child process finishes before continuing
                    $status = $_status = null;
                    \pcntl_waitpid($worker, $status, WNOHANG);

                    if (\pcntl_wifexited($status) !== true) {
                        // Checks if  status code of child process represents a normal exit

                        $this->log('JobFailedAbnormally', \compact('status'));
                        // job failed
                        if (\method_exists($job, 'onFailed')) {
                            try {
                                $job->onFailed();
                            } catch (Throwable $th) {
                                $this->log('JobOnFailedCallbackFailedWhenAbnormally', Format::throwable($th));
                            }
                        }
                    } elseif (($code = \pcntl_wexitstatus($status)) !== 0) {
                        // Check the return code of a terminated child
                        // \pcntl_wexitstatus() is only useful if \pcntl_wifexited() returned TRUE

                        $this->log('JobFailedExitUnexpectedStatusCode', \compact('code'));
                        if (\method_exists($job, 'onFailed')) {
                            try {
                                $job->onFailed();
                            } catch (Throwable $th) {
                                $this->log('JobOnFailedCallbackFailedWithBadCode', Format::throwable($th));
                            }
                        }
                    } else {
                        $this->log('ProcessedSuccessfully', ['job' => \get_class($job)]);
                    }

                    \pcntl_waitpid($worker, $_status);    // Avoid defunct process of parant
                    \posix_kill($worker, SIGKILL);
                } elseif ($worker === 0) {
                    // Child process
                    $code = Worker::process($job, function ($th) {
                        $this->logger()->log('queue-worker-error', 'JobExecuteError', [$this->queue => Format::throwable($th)]);
                    });
                    exit($code);    // Exit normally as child process and don't mess other jobs
                }

                unset($worker);
            }

            unset($job);
        };
    }

    public function log(string $message, array $context = [])
    {
        if ($this->quiet) {
            $this->logger()->log('queue-dispatcher', $message, [$this->queue => $context]);
            return;
        }

        $this->console->line(JSON::encode([Format::microtime('T Y-m-d H:i:s', '.'), $this->queue, $message, $context]));
    }

    /**
     * Setter for console
     *
     * @param Console $console
     * @return Dispatcher
     */
    public function setConsole(Console $console)
    {
        $this->console = $console;
    
        return $this;
    }

    /**
     * Setter for queuable
     *
     * @param Queuable $queuable
     * @return Dispatcher
     */
    public function setQueuable(Queuable $queuable)
    {
        $this->queuable = $queuable;
    
        return $this;
    }

    /**
     * Setter for queue
     *
     * @param string $queue
     * @return Dispatcher
     */
    public function setQueue(string $queue)
    {
        $this->queue = $queue;
    
        return $this;
    }

    /**
     * Setter for daemon
     *
     * @param bool $daemon
     * @return Dispatcher
     */
    public function setDaemon(bool $daemon)
    {
        $this->daemon = $daemon;
    
        return $this;
    }

    /**
     * Setter for quiet
     *
     * @param bool $quiet
     * @return Dispatcher
     */
    public function setQuiet(bool $quiet)
    {
        $this->quiet = $quiet;
    
        return $this;
    }

    /**
     * Setter for debug
     *
     * @param bool $debug
     * @return Dispatcher
     */
    public function setDebug(bool $debug)
    {
        $this->debug = $debug;
    
        return $this;
    }

    /**
     * Setter for timeout
     *
     * @param int $timeout
     * @return Dispatcher
     */
    public function setTimeout(int $timeout)
    {
        $this->timeout = $timeout;
    
        return $this;
    }

    /**
     * Setter for interval
     *
     * @param int $interval
     * @return Dispatcher
     */
    public function setInterval(int $interval)
    {
        $this->interval = $interval;
    
        return $this;
    }

    /**
     * Setter for tryDelay
     *
     * @param int $tryDelay
     * @return Dispatcher
     */
    public function setTryDelay(int $tryDelay)
    {
        $this->tryDelay = $tryDelay;
    
        return $this;
    }

    /**
     * Setter for tryTimes
     *
     * @param int $tryTimes
     * @return Dispatcher
     */
    public function setTryTimes(int $tryTimes)
    {
        $this->tryTimes = $tryTimes;
    
        return $this;
    }
}
