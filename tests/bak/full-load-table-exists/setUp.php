<?php

declare(strict_types=1);

use Keboola\DbWriter\Exasol\FunctionalTests\DatadirTest;
use Keboola\DbWriter\Exasol\FunctionalTests\DatabaseSetupManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseSetupManager($test->getConnection());
    $tableName = 'simple';
    $manager->createSimpleTable($tableName);
    $manager->insertRows($tableName, $manager->getSimpleTableColumns(), [
        [4, 'A', null],
        [5, 'B', null],
        [6, 'C', null],
    ]);
};
