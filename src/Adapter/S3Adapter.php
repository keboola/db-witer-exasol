<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Adapter;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\S3\SourceDirectory;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

class S3Adapter implements IAdapter
{
    private Connection $connection;

    private bool $isSliced;

    private string $region;

    private string $bucket;

    private string $key;

    private string $accessKeyId;

    private string $secretAccessKey;

    private string $sessionToken;

    public function __construct(array $s3info)
    {
        $this->isSliced = $s3info['isSliced'];
        $this->region = $s3info['region'];
        $this->bucket = $s3info['bucket'];
        $this->key = $s3info['key'];
        $this->accessKeyId = $s3info['credentials']['access_key_id'];
        $this->secretAccessKey = $s3info['credentials']['secret_access_key'];
        $this->sessionToken = $s3info['credentials']['session_token'];
    }

    public function importToStagingTable(string $schemaName, string $tableName): void {
        $columns = new ColumnCollection();
        $primaryKeys = [];
        $csvOptions = new CsvOptions();
        $source = new SourceDirectory(
            $this->accessKeyId,
            $this->secretAccessKey,
            $this->region,
            $this->bucket,
            $this->key,
            $csvOptions,
            $this->isSliced,
        );
        $options = new ExasolImportOptions();
        $target = new ExasolTableDefinition($schemaName, $tableName, true, $columns, $primaryKeys);
        $importer = new ToStageImporter($this->connection);
        $importer->importToStagingTable($source, $target, $options);
    }
}
