<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use Keboola\Csv\CsvFile;
use Keboola\ExasolWriter\ExasolWriter;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use PDO;

trait DumpTablesTrait
{
    abstract public function getConnection(): PDO;

    public function dumpTable(string $schema, string $name, string $dumpDir): void
    {
        $metadata = $this->getTableMetadata($schema, $name);
        $columns = $this->getColumns($schema, $name);
        $metadata['columns'] = $columns;

        // Save create statement
        $metadataJson = json_encode($metadata, JSON_PRETTY_PRINT);
        file_put_contents(sprintf('%s/%s.%s.metadata.json', $dumpDir, $schema, $name), $metadataJson);

        // Dump data
        $this->dumpTableData($schema, $name, $columns, $dumpDir);
    }

    private function dumpTableData(
        string $schema,
        string $name,
        array $columns,
        string $dumpDir
    ): void {
        $csv = new CsvFile(sprintf('%s/%s.%s.data.csv', $dumpDir, $schema, $name));

        // Write header
        $csv->writeRow(array_map(
            fn($col) => $col['name'],
            $columns
        ));

        $orderColumns = [ExasolQuote::quoteSingleIdentifier($columns[0]['name'])];
        // order by first two columns
        if (isset($columns[1])) {
            $orderColumns[] = ExasolQuote::quoteSingleIdentifier($columns[1]['name']);
        }

        // Write data
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM %s.%s ORDER BY %s',
            ExasolQuote::quoteSingleIdentifier($schema),
            ExasolQuote::quoteSingleIdentifier($name),
            implode(', ', $orderColumns)
        ));
        /** @var array $rows */
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $csv->writeRow($row);
        }
    }

    private function getTableMetadata(string $schema, string $name): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM "EXA_ALL_TABLES" ' .
            'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s',
            ExasolQuote::quote($schema),
            ExasolQuote::quote($name),
        ));

        /** @var array $result */
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        return [
            'name' => $result['TABLE_NAME'],
            'schema' => $result['TABLE_SCHEMA'],
        ];
    }

    private function getColumns(string $schema, string $name): array
    {
        /** @var \PDOStatement $stmt */
        $stmt = $this->getConnection()->query(sprintf(
            'SELECT * FROM "EXA_ALL_COLUMNS" ' .
            'WHERE COLUMN_SCHEMA = %s AND COLUMN_TABLE = %s',
            ExasolQuote::quote($schema),
            ExasolQuote::quote($name),
        ));

        /** @var array $result */
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return array_map(function (array $column) {
            return [
                'name' => $column['COLUMN_NAME'],
                'position' => $column['COLUMN_ORDINAL_POSITION'],
                'type' => $column['COLUMN_TYPE'],
                'is_nullable' => $column['COLUMN_IS_NULLABLE'] !== '0',
                'default' => $column['COLUMN_DEFAULT'],
            ];
        }, $result);
    }
}
