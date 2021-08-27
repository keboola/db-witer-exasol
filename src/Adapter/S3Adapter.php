<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Adapter;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

class S3Adapter implements Adapter
{
    private Connection $connection;

    private bool $isSliced;

    private string $region;

    private string $bucket;

    private string $key;

    private string $accessKeyId;

    private string $secretAccessKey;

    private string $sessionToken;

    public function __construct(Connection $connection, array $s3info)
    {
        $this->connection = $connection;
        $this->isSliced = $s3info['isSliced'];
        $this->region = $s3info['region'];
        $this->bucket = $s3info['bucket'];
        $this->key = $s3info['key'];
        $this->accessKeyId = $s3info['credentials']['access_key_id'];
        $this->secretAccessKey = $s3info['credentials']['secret_access_key'];
        $this->sessionToken =  $s3info['credentials']['session_token'];
    }

    public function importToStagingTable(ExasolTableDefinition $table): void {
        $csvOptions = new CsvOptions();
        $source = new S3SourceFile(
            $this->accessKeyId,
            $this->secretAccessKey,
            $this->sessionToken,
            $this->region,
            $this->bucket,
            $this->key,
            $csvOptions,
            $this->isSliced,
            $table->getColumnsNames(),
            $table->getPrimaryKeysNames(),
        );
        $options = new ExasolImportOptions();
        $importer = new ToStageImporter($this->connection);
        $importer->importToStagingTable($source, $table, $options);
    }
}
