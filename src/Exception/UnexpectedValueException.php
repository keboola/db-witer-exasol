<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Exception;

use Keboola\CommonExceptions\UserExceptionInterface;

class UnexpectedValueException extends \Exception implements UserExceptionInterface
{

}
