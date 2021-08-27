<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use PDO;
use Keboola\ExasolWriter\ExasolWriter;

trait InsertRowsTrait
{
    abstract public function getConnection(): PDO;

    public function insertRows(string $tableName, array $columns, array $rows): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = ExasolHelper::quoteIdentifier($name);
        }

        // Generate values statement
        $valuesSql = [];
        foreach ($rows as $row) {
            $valuesSql[] =
                '(' .
                implode(
                    ', ',
                    array_map(fn($value) => $value === null ? 'NULL' : ExasolHelper::quote((string) $value), $row)
                ) .
                ')';
        }

        // Insert values
        foreach ($valuesSql as $values) {
            $this->getConnection()->exec(sprintf(
                'INSERT INTO %s (%s) VALUES %s',
                ExasolHelper::quoteIdentifier($tableName),
                implode(', ', $columnsSql),
                $values
            ));
        }
    }
}
