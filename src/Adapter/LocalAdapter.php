<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Adapter;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

class LocalAdapter implements Adapter
{
    private Connection $connection;

    private string $csvFile;

    public function __construct(Connection $connection, string $csvFile)
    {
        $this->connection = $connection;
        $this->csvFile = $csvFile;
    }


    public function importToStagingTable(ExasolTableDefinition $table): void
    {
        $csvOptions = new CsvOptions();
        $sql = $this->getCopyCommand($table, $csvOptions);
        $this->connection->executeQuery($sql);
    }

    private function getCopyCommand(
        ExasolTableDefinition $destination,
        CsvOptions $csvOptions
    ): string {
        $destinationSchema = ExasolQuote::quoteSingleIdentifier($destination->getSchemaName());
        $destinationTable = ExasolQuote::quoteSingleIdentifier($destination->getTableName());


        return sprintf(
            '
IMPORT INTO %s.%s FROM LOCAL CSV FILE %s
--- file_opt
COLUMN SEPARATOR=%s
COLUMN DELIMITER=%s
',
            $destinationSchema,
            $destinationTable,
            ExasolQuote::quote($this->csvFile),
            ExasolQuote::quote($csvOptions->getDelimiter()),
            ExasolQuote::quote($csvOptions->getEnclosure())
        );
    }
}
