<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Adapter;

use RuntimeException;

class NullAdapter implements IAdapter
{
    public function importToStagingTable(string $schemaName, string $tableName): void {
        throw new RuntimeException('Null adapter used.');
    }
}
