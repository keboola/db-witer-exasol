<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use PDO;

trait GetAllTablesTrait
{
    abstract public function getConnection(): PDO;

    public function getAllTables(): array
    {
        $connection = $this->getConnection();
        $sql = sprintf(
            'SELECT * FROM "EXA_ALL_TABLES" WHERE TABLE_SCHEMA=%s',
            ExasolQuote::quote((string) getenv('EXASOL_SCHEMA')),
        );

        /** @var \PDOStatement $stmt */
        $stmt = $connection->query($sql);
        /** @var array $tables */
        $tables = $stmt->fetchAll();
        return $tables;
    }
}
