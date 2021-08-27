<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Test;

use Keboola\ExasolWriter\Application;
use Keboola\ExasolWriter\Writer;
use Keboola\Temp\Temp;
use RuntimeException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;

class StagingStorageLoader
{
    private const CACHE_PATH = __DIR__ . '/../../tests/.cache/StagingStorageLoader.cache.json';

    private array $fileIdCache;

    private string $dataDir;

    private Client $storageApi;

    private Temp $temp;

    public function __construct(string $dataDir, Client $storageApiClient)
    {
        $this->dataDir = $dataDir;
        $this->storageApi = $storageApiClient;
        $this->fileIdCache = @json_decode((string) @file_get_contents(self::CACHE_PATH), true) ?: [];
        $this->temp = new Temp();
    }

    public function __destruct()
    {
        file_put_contents(self::CACHE_PATH, @json_encode($this->fileIdCache));
        $this->temp->remove();
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    public function upload(string $tableId, string $csvPath, array $manifest, string $testName): array
    {
        // Create CSV file with header
        $tablePath = $this->temp->createFile($testName);
        $header = '"' . implode('","', $manifest['columns']) . '"' . "\n";
        $rows = (string) file_get_contents($csvPath);
        file_put_contents($tablePath->getPathname(), $header . $rows);
        $csvTable = new CsvFile($tablePath);

        // Load from cache
        $cacheKey = $testName . '-' . $tableId;
        if (isset($this->fileIdCache[$cacheKey])) {
            $fileId = $this->fileIdCache[$cacheKey];
            try {
                $fileInfo = $this->storageApi->getFile($fileId, (new GetFileOptions())->setFederationToken(true));
                return [
                    'fileId' => $fileId,
                    'stagingStorage' => Writer::STORAGE_S3,
                    'manifest' => $this->getS3Manifest($fileInfo),
                ];
            } catch (\Throwable $e) {
                // re-upload if an error
            }
        }

        // Create bucket
        $filePath = $this->getInputCsv($tableId);
        $bucketId = 'test-wr-db-exasol';
        $fullTableId = 'in.c-' . $bucketId . '.' . $tableId;
        if (!$this->storageApi->bucketExists('in.c-' . $bucketId)) {
            $this->storageApi->createBucket($bucketId, Client::STAGE_IN);
        }

        // Create table
        if ($this->storageApi->tableExists($fullTableId)) {
            $this->storageApi->dropTable($fullTableId);
        }
        $sourceTableId = $this->storageApi->createTable('in.c-' .$bucketId, $tableId, $csvTable);

        // Upload
        $this->storageApi->writeTable($sourceTableId, $csvTable);
        $job = $this->storageApi->exportTableAsync($sourceTableId, ['gzip' => true]);
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );

        if (!isset($fileInfo['s3Path'])) {
            throw new RuntimeException('Only S3 staging storage is supported.');
        }

        $result = [
            'fileId' => $job['file']['id'],
            'stagingStorage' => Writer::STORAGE_S3,
            'manifest' => $this->getS3Manifest($fileInfo),
        ];

        $this->fileIdCache[$cacheKey] = $job['file']['id'];
        return $result;
    }

    private function getS3Manifest(array $fileInfo): array
    {
        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['isSliced'] ? $fileInfo['s3Path']['key'] . 'manifest' : $fileInfo['s3Path']['key'],
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }
}
