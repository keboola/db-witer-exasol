<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Adapter;

use Aws\S3\S3Client;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;

class S3SourceFile extends SourceFile
{
    protected string $token;

    public function __construct(
        string $key,
        string $secret,
        string $token,
        string $region,
        string $bucket,
        string $filePath,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = [],
        ?array $primaryKeysNames = null
    ) {
        parent::__construct(
            $key,
            $secret,
            $region,
            $bucket,
            $filePath,
            $csvOptions,
            $isSliced,
            $columnsNames,
            $primaryKeysNames,
        );
        $this->token = $token;
    }

    protected function getClient(): S3Client
    {
        return new S3Client([
            'credentials' => [
                'key' => $this->getKey(),
                'secret' => $this->getSecret(),
                'token' => $this->token
            ],
            'region' => $this->getRegion(),
            'version' => '2006-03-01',
        ]);
    }
}
