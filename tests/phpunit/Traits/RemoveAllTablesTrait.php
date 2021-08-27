<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
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
                ExasolQuote::quoteSingleIdentifier($table['TABLE_SCHEMA']),
                ExasolQuote::quoteSingleIdentifier($table['TABLE_NAME'])
            ));
        }
    }
}
