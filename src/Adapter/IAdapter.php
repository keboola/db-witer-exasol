<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Adapter;

interface IAdapter
{
    public function importToStagingTable(string $schemaName, string $tableName): void;
}
