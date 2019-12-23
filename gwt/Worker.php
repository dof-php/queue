<?php

$gwt->unit('Test Woker::process()', function ($t) {
    $t->exceptor(function () {
        \DOF\Queue\Worker::process((new \DOF\Queue\Test\InvalidJob), function ($th) {
		});
    }, 'TypeError');
    $t->eq(\DOF\Queue\Worker::process((new \DOF\Queue\Test\ExceptionJob), function ($th) {
    }), -1);
    $t->eq(\DOF\Queue\Worker::process((new \DOF\Queue\Test\NormalJob), function ($th) {
    }), 0);
});
