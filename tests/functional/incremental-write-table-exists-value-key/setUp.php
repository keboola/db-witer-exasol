<?php

declare(strict_types=1);

use Keboola\ExasolWriter\FunctionalTests\DatadirTest;
use Keboola\ExasolWriter\FunctionalTests\DatabaseSetupManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseSetupManager($test->getConnection());
    $manager->createIncrementalTable();
    $manager->generateIncrementalTableRows();
};
