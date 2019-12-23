<?php

declare(strict_types=1);

namespace DOF\Queue;

use Throwable;
use DOF\ENV;
use DOF\DMN;
use DOF\Convention;
use DOF\Util\IS;
use DOF\Util\FS;
use DOF\Util\JSON;
use DOF\Util\Reflect;
use DOF\DDD\Model;
use DOF\DDD\Entity;
use DOF\DDD\Event;
use DOF\DDD\Listenable;
use DOF\DDD\ModelManager;
use DOF\DDD\EntityManager;
use DOF\DDD\QueueAdaptor;
use DOF\Queue\Dispatcher;

class Command
{
    /**
     * @CMD(event.queues)
     * @Desc(Check and get queues list of event origin in current environment for deployment)
     * @Option(ddd){notes=DDD builtin event: entity/model}
     * @Argv(1){notes=The event class path}
     */
    public function getEventQueues($console)
    {
        $origin = $console->first();
        if (IS::empty($origin)) {
            $console->fail('Missing event origin');
            return;
        }
        if (! \is_file($origin)) {
            $console->fail('Event origin not exists', \compact('origin'));
            return;
        }
        $origin = Reflect::getFileNamespace($origin, true);
        if (! $origin) {
            $console->error('Invalid event origin', \compact('origin'));
            return;
        }
        $events = [];
        if (\is_subclass_of($origin, Event::class)) {
            $events = [$origin];
        } else {
            $meta = [];
            if ($entity = \is_subclass_of($origin, Entity::class)) {
                $meta = EntityManager::get($origin)['meta']['doc'] ?? [];
            } elseif (\is_subclass_of($origin, Model::class)) {
                $meta = ModelManager::get($origin)['meta']['doc'] ?? [];
            } else {
                $console->error('Empty or invalid DDD builtin event type', \compact('origin', 'ddd'));
                return;
            }
            foreach ([
                Model::ON_CREATED => $entity ? \DOF\DDD\Event\EntityCreated::class : \DOF\DDD\Event\ModelCreated::class,
                Model::ON_REMOVED => $entity ? \DOF\DDD\Event\EntityRemoved::class : \DOF\DDD\Event\ModelRemoved::class,
                Model::ON_UPDATED => $entity ? \DOF\DDD\Event\EntityUpdated::class : \DOF\DDD\Event\ModelUpdated::class,
            ] as $name => $event) {
                if ($meta[$name] ?? null) {
                    $events[] = $event;
                }
            }
        }

        foreach ($events as $event) {
            $console->line(\join(' ', ['#', $event]));
            $domain = DMN::name($origin);
            $async = ENV::final($domain, Event::ASYNC_OPTION, []);
            if ((! $async) || (! ($async[$event] ?? null))) {
                $console->warn('Not an async event', \compact('event'));
                continue;
            }
            $driver = ENV::finalMatch($origin, [Event::QUEUE_DRIVER, QueueAdaptor::QUEUE_DRIVER]);
            $queue = QueueAdaptor::name($event, 0, Event::QUEUE_PREFIX);
            $options = "--domain={$domain} --driver={$driver} --queue={$queue}";
            if ($queue === QueueAdaptor::QUEUE_DEFAULT) {
                $console->line($options);
                continue;
            }
            $partition = $async[$event] ?? (\in_array($event, $async) ? 0 : null);
            if ((null !== $partition) && (false !== $partition)) {
                $partition = ($partition === true) ? 0 : $partition;
                if (! \is_int($partition)) {
                    $console->error('Invalid async event partition integer', \compact('event', 'partition'));
                    return;
                }
            }
            if ($partition < 1) {
                $console->line($options);
                continue;
            }
            for ($i = 0; $i < $partition; $i++) {
                $console->line(\join('_', [$options, $i]));
            }
        }
    }

    /**
     * @CMD(listener.queues)
     * @Desc(Check and get queues list of a listener origin in current environment for deployment)
     * @Argv(1){notes=The listener class path}
     */
    public function getListenerQueues($console)
    {
        if (IS::empty($origin = $console->first())) {
            $console->fail('Missing listener origin');
            return;
        }
        if (! \is_file($origin)) {
            $console->fail('Listener origin not exists', \compact('origin'));
            return;
        }
        if (! ($origin = Reflect::getFileNamespace($origin, true))) {
            $console->error('Invalid listener origin without namespace', \compact('origin'));
            return;
        }
        if (! \is_subclass_of($origin, Listenable::class)) {
            $console->error('Invalid listener');
            return;
        }

        $domain = DMN::name($origin);
        $async = ENV::final($domain, Listenable::ASYNC_OPTION, []);
        if ((! $async) || (! ($async[$origin] ?? null))) {
            $console->warn('Not an async listener', \compact('origin'));
            return;
        }
        $driver = ENV::finalMatch($origin, [Listenable::QUEUE_DRIVER, QueueAdaptor::QUEUE_DRIVER]);
        if (IS::empty($driver)) {
            $console->error('Empty or Invalid listener queue driver config', \compact('driver'));
            return;
        }

        $queue = QueueAdaptor::name($origin, 0, Listenable::QUEUE_PREFIX);
        $options = "--domain={$domain} --driver={$driver} --queue={$queue}";
        if ($queue === QueueAdaptor::QUEUE_DEFAULT) {
            $console->line($options);
            return;
        }
        $partition = $async[$origin] ?? (\in_array($origin, $async) ? 0 : null);
        if ((null !== $partition) && (false !== $partition)) {
            $partition = ($partition === true) ? 0 : $partition;
            if (! \is_int($partition)) {
                $console->error('Invalid async listener partition integer', \compact('listener', 'partition'));
                return;
            }
        }
        if ($partition < 1) {
            $console->line($options);
            return;
        }
        for ($i = 0; $i < $partition; $i++) {
            $console->line(\join('_', [$options, $i]));
        }
    }

    /**
     * @CMD(queue.run)
     * @Desc(Start a queue worker on a queue in a domain)
     * @Option(domain){notes=Domain name where the origin of queue name}
     * @Option(driver){notes=Queue driver stores queue jobs}
     * @Option(queue){notes=Queue name to listen}
     * @Option(once){notes=Pop the first job of the queue and exit after that job finished}
     * @Option(quiet){notes=Do not print any output to console}
     * @Option(debug){notes=Queue dispatcher self as job worker}
     * @Option(daemon){notes=Run queue workers as daemon, required restart if code updated&default=true}
     * @Option(interval){notes=Seconds to wait to re-check jobs if no jobs in current queue}
     * @Option(timeout){notes=Max seconds allowed for each job worker to execute}
     * @Option(try-times){notes=Max re-execute times when job failed}
     * @Option(try-delay){notes=Seconds of time to wait for re-executing after job failed}
     */
    public function queueRun($console)
    {
        $domain = $console->getOption('domain', null, true);
        $driver = $console->getOption('driver', null, true);
        $queue = $console->getOption('queue', null, true);
        // Get a queuable instance first
        $queuable = QueueAdaptor::get($driver, $domain, $queue, $console->tracer(), false);
        if (! $queuable) {
            $console->exceptor('NoQueuableFoundInGivenDomainDriverAndName', \compact('domain', 'driver', 'queue'));
        }
        if ($console->hasOption('once')) {
            $job = $queuable->dequeue($queue);
            if (! $job) {
                $console->info('NoJobOnCurrentQueue', \compact('domain', 'queue', 'driver'));
                return;
            }
            try {
                $job->execute();
            } catch (Throwable $th) {
                $console->exceptor('JobExceptionFoundWhenOncing', \compact('queue'), $th);
            }
            $console->success("Queue job {$queue} on {$driver} has executed once.");
            return;
        }

        $interval = (int) $console->getOption('interval', 3);
        $timeout  = (int) $console->getOption('timeout', 0);
        $tryTimes = (int) $console->getOption('try-times', 0);
        $tryDelay = (int) $console->getOption('try-delay', -1);

        // concel all kinds of shutdown callbacks of CLI kernel
        $console->__TRACE_ROOT__->unregister('before-shutdown');
        $console->__TRACE_ROOT__->unregister('shutdown');

        // Start a queue scheduler and looping jobs for workers on that queue
        $console->new(Dispatcher::class)
        // (new Dispatcher)
            ->setQueuable($queuable)
            ->setConsole($console)
            ->setQueue($queue)
            ->setQuiet($console->hasOption('quiet'))
            ->setDebug($console->hasOption('debug'))
            ->setDaemon($console->hasOption('daemon'))
            ->setInterval($interval)
            ->setTimeout($timeout)
            ->setTryTimes($tryTimes)
            ->setTryDelay($tryDelay)
            ->looping();
    }

    /**
     * @CMD(queue.restart)
     * @Desc(Restart a queue worker on a queue name of a domain origin)
     * @Option(domain){notes=Domain name where the origin of queue name}
     * @Option(driver){notes=Queue driver stores queue jobs}
     * @Option(queue){notes=Queue name to restart}
     */
    public function queueRestart($console)
    {
        $domain = $console->getOption('domain', null, true);
        $driver = $console->getOption('driver', null, true);
        $queue  = $console->getOption('queue', null, true);

        // Get a queuable instance first
        $queuable = QueueAdaptor::get($driver, $domain, $queue, $console->tracer());
        $queuable->setRestart($queue) ? $console->success() : $console->fail();
    }
}
