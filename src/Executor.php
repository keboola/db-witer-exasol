<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class Executor
{
    private Connection $connection;

    private ExasolTableQueryBuilder $queryBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->queryBuilder = new ExasolTableQueryBuilder();
    }

    public function createIfNotExists(ExasolTableDefinition $table): void
    {
        $sql = $this->queryBuilder->getCreateTableCommandFromDefinition(
            $table, TableQueryBuilderInterface::CREATE_TABLE_WITH_PRIMARY_KEYS
        );
        $sql = preg_replace('~^CREATE TABLE~', 'CREATE TABLE IF NOT EXISTS', $sql);
        $this->connection->executeQuery($sql);
    }

    public function create(ExasolTableDefinition $table): void
    {
        $sql = $this->queryBuilder->getCreateTableCommandFromDefinition(
            $table, TableQueryBuilderInterface::CREATE_TABLE_WITH_PRIMARY_KEYS
        );
        $this->connection->executeQuery($sql);
    }

    public function drop(ExasolTableDefinition $table): void
    {
        $sql = sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            ExasolQuote::quoteSingleIdentifier($table->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($table->getTableName())
        );
        $this->connection->executeQuery($sql);
    }

    public function swapTables(ExasolTableDefinition $tableA, ExasolTableDefinition $tableB): void
    {

    }
}
