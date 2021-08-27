<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Configuration;

use Keboola\Component\Config\BaseConfig;
use Keboola\Datatype\Definition\Exasol;
use Keboola\DbWriter\Exception\UserException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

class Config extends BaseConfig
{
    public function getHost(): string {
        return  (string) $this->getValue(['parameters', 'db', 'host']);
    }

    public function getPort(): int {
        return  (int) $this->getValue(['parameters', 'db', 'port']);
    }

    public function getUsername(): string {
        return  (string) $this->getValue(['parameters', 'db', 'user']);
    }

    public function getPassword(): string {
        return  (string) $this->getValue(['parameters', 'db', '#password']);
    }

    public function getSchema(): string {
        return  (string) $this->getValue(['parameters', 'db', 'schema']);
    }

    public function isIncremental(): bool
    {
        return (bool)$this->getValue(['parameters', 'incremental']);
    }

    public function getTableId(): string {
        return  (string) $this->getValue(['parameters', 'tableId']);
    }

    public function getTableName(): string {
        return  (string) $this->getValue(['parameters', 'dbName']);
    }

    public function getTable(array $csvHeader): ExasolTableDefinition {
        return new ExasolTableDefinition(
            $this->getSchema(),
            $this->getTableName(),
            false,
            $this->getColumns($csvHeader),
            (array) $this->getValue(['parameters', 'primaryKey']),
        );
    }

    /**
     * Return columns ordered by CSV header.
     */
    public function getColumns(array $csvHeader): ColumnCollection
    {
        $config = $this->getValue(['parameters', 'items']);
        $names = [];
        $columns = [];

        // Map columns to objects
        foreach ($csvHeader as $csvName) {
            foreach ($config as $data) {
                if ($csvName === $data['name']) {
                    $columns[] = $this->getColumn($data);
                    $names[] = $data['name'];
                    continue 2;
                }
            }
        }

        // Check all columns found
        foreach ($config as $data) {
            $name = $data['name'];
            if (!in_array($name, $names)) {
                throw new UserException(sprintf('Column "%s", defined in the config, is missing in the CSV table.', $name));
            }
        }

        return new ColumnCollection($columns);
    }

    protected function getColumn(array $data): ExasolColumn
    {
        $type = $data['type'];
        $options = [
            'nullable' => $data['nullable'] ?? null,
            'default' => $data['default'] ?? null,
        ];
        if (!in_array($type, Exasol::TYPES_WITHOUT_LENGTH)) {
            $options['length'] = $data['size'] ?? null;
        }
        return new ExasolColumn($data['name'], new Exasol($type, $options));
    }
}
