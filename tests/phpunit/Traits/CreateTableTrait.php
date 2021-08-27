<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use Keboola\ExasolWriter\ExasolWriter;
use PDO;

trait CreateTableTrait
{
    abstract public function getConnection(): PDO;

    public function createTable(string $tableName, array $columns): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = ExasolHelper::quoteIdentifier($name) . ' ' . $sqlDef;
        }

        // Create table
        $this->getConnection()->exec(sprintf(
            'CREATE TABLE %s (%s)',
            ExasolHelper::quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        ));
    }
}
