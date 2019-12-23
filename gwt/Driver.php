<?php

$gwt->unit('Test Driver::support()', function ($t) {
	$driver = 'rEDIs';
	$t->true(\DOF\Queue\Driver::support($driver));
	$t->eq($driver, 'redis');

	$driver = 'mysql';
	$t->false(\DOF\Queue\Driver::support($driver));

	$driver = 'memcahed';
	$t->false(\DOF\Queue\Driver::support($driver));
});
