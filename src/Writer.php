<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\DbWriter\Exception\UserException;
use Keboola\ExasolWriter\Adapter\Adapter;
use Keboola\ExasolWriter\Adapter\LocalAdapter;
use Keboola\ExasolWriter\Adapter\S3Adapter;
use Keboola\ExasolWriter\Configuration\Config;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Psr\Log\LoggerInterface;

class Writer
{
    public const STORAGE_S3 = 's3';
    public const STAGE_NAME = '_db_writer_stage';

    private Config $config;

    private string $dataDir;

    private LoggerInterface $logger;

    private Connection $connection;

    private Executor $executor;

    private ImportState $importState;

    private array $manifest;

    private Adapter $adapter;

    private ExasolTableDefinition $table;

    private ExasolTableDefinition $stagingTable;

    public function __construct(Config $config, string $dataDir, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->dataDir = $dataDir;
        $this->logger = $logger;
        $this->connection = $this->createConnection();
        $this->executor = new Executor($this->connection);
    }

    public function testConnection(): void
    {
        $this->connection->executeQuery('SELECT 1');
    }

    public function write(): void
    {
        // Create table
        $this->manifest = $this->getManifest();
        $this->table = $this->config->getTable($this->manifest['columns']);
        if ($this->table->getColumnsDefinitions()->count() == 0) {
            throw new UserException('No columns specified.');
        }

        // Create staging table
        $this->stagingTable = $this->getStagingTable();
        $this->writeToStaging();


        // Write
        $this->executor->createIfNotExists($this->table);
        $this->writeToTarget();
    }

    protected function createConnection(): Connection {
        $factory = new ConnectionFactory($this->logger);
        return $factory->create($this->config);
    }

    protected function createAdapter(): Adapter
    {
        $tableId = $this->config->getTableId();
        $csvFile = $this->dataDir . '/in/tables/' . $tableId . '.csv';
        if (isset($this->manifest[self::STORAGE_S3])) {
            return new S3Adapter($this->connection, $this->manifest[self::STORAGE_S3]);
        } else if (file_exists($csvFile)){
            return new LocalAdapter($this->connection, $csvFile);
        }
        throw new UserException('Unknown staging storage');
    }

    protected function writeToStaging(): void {
        // Create empty staging table
        $this->executor->drop($this->stagingTable);
        $this->executor->create($this->stagingTable);

        // Import to staging table
        $this->adapter = $this->createAdapter();
        $this->adapter->importToStagingTable($this->stagingTable);
    }

    protected function writeToTarget(): void {
        $this->importState = new ImportState($this->stagingTable->getTableName());
        if ($this->config->isIncremental()) {
            $this->writeIncremental();
        } else {
            $this->writeFull();
        }
    }

    protected function writeIncremental(): void
    {
        $options = new ExasolImportOptions();
        $incrementalImporter = new IncrementalImporter($this->connection);

        try {
            $incrementalImporter->importToTable($this->stagingTable, $this->table, $options, $this->importState);
        } finally {
            $this->executor->drop($this->stagingTable);
        }
    }

    protected function writeFull(): void
    {
        $options = new ExasolImportOptions();
        $fullImporter = new FullImporter($this->connection);

        try {
            $fullImporter->importToTable($this->stagingTable, $this->table, $options, $this->importState);
            $this->executor->swapTables($this->stagingTable, $this->table);
        } finally {
            $this->executor->drop($this->stagingTable);
        }
    }

    protected function getManifest(): array
    {
        $tableId = $this->config->getTableId();
        $tableManifestPath = $this->dataDir . '/in/tables/' . $tableId . '.csv.manifest';
        if (!file_exists($tableManifestPath)) {
            throw new UserException(sprintf(
                'Table "%s" in storage input mapping cannot be found.',
                $tableId
            ));
        }
        return json_decode(
            (string)file_get_contents($tableManifestPath),
            true
        );
    }

    protected function getStagingTable(): ExasolTableDefinition
    {
        return new ExasolTableDefinition(
            $this->table->getSchemaName(),
            $this->table->getTableName(),
            true,
            $this->table->getColumnsDefinitions(),
            $this->table->getPrimaryKeysNames(),
        );
    }

    protected function generateStageName(): string
    {
        return rtrim(
            mb_substr(
                sprintf(
                    '%s_%s',
                    self::STAGE_NAME,
                    str_replace('.', '_', $this->config->getTableName())
                ),
                0,
                255
            ),
            '-'
        );
    }
}
