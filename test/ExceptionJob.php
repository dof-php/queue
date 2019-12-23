<?php

declare(strict_types=1);

namespace DOF\Queue\Test;

use DOF\Util\Task;
use DOF\Util\Exceptor;

class ExceptionJob implements Task
{
    public function execute()
    {
        throw new Exceptor('ExceptionJobExceptorTestName');
    }
}
