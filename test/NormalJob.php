<?php

declare(strict_types=1);

namespace DOF\Queue\Test;

use DOF\Util\Task;
use DOF\Util\Exceptor;

class NormalJob implements Task
{
    public function execute()
    {
        // do sth
    }
}
