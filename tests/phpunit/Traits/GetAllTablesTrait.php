<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Tests\Traits;

use Keboola\DbWriter\Exasol\ExasolHelper;
use PDO;

trait GetAllTablesTrait
{
    abstract public function getConnection(): PDO;

    public function getAllTables(): array
    {
        $connection = $this->getConnection();
        $sql = sprintf(
            'SELECT * FROM "EXA_ALL_TABLES" WHERE TABLE_SCHEMA=%s',
            ExasolHelper::quote((string) getenv('EXASOL_SCHEMA')),
        );

        /** @var \PDOStatement $stmt */
        $stmt = $connection->query($sql);
        /** @var array $tables */
        $tables = $stmt->fetchAll();
        return $tables;
    }
}
