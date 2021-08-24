<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Tests\Traits;

use Keboola\DbWriter\Exasol\ExasolHelper;
use PDO;

trait RemoveAllTablesTrait
{
    use GetAllTablesTrait;

    abstract public function getConnection(): PDO;

    public function removeAllTables(): void
    {
        foreach ($this->getAllTables() as $table) {
            $this->connection->exec(sprintf(
                'DROP TABLE %s.%s',
                ExasolHelper::quoteIdentifier($table['TABLE_SCHEMA']),
                ExasolHelper::quoteIdentifier($table['TABLE_NAME'])
            ));
        }
    }
}
