<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol;

class ExasolHelper
{
    public static function quote(string $value): string
    {
        $q = "'";
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public static function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

}
