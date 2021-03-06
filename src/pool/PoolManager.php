<?php

namespace phpboot\dal\pool;

use phpboot\common\swoole\Swoole;
use phpboot\common\util\StringUtils;
use phpboot\dal\ConnectionBuilder;
use phpboot\dal\ConnectionInterface;
use Throwable;

final class PoolManager
{
    private static array $map1 = [];

    private function __construct()
    {
    }

    public static function addPool(PoolInterface $pool, ?int $workerId = null): void
    {
        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return;
        }

        $poolType = StringUtils::substringBefore($pool->getPoolId(), ':');
        $key = "{$poolType}_pool_worker$workerId";
        self::$map1[$key] = $pool;
    }

    public static function getPool(string $poolType, ?int $workerId = null): ?PoolInterface
    {
        if (!is_int($workerId)) {
            $workerId = Swoole::getWorkerId();
        }

        if (!is_int($workerId) || $workerId < 0) {
            return null;
        }

        $key = "{$poolType}_pool_worker$workerId";
        $pool = self::$map1[$key];
        return $pool instanceof PoolInterface ? $pool : null;
    }

    public static function withPoolInfo(string $poolType, PoolInfo $poolInfo): void
    {
        $key = "{$poolType}_pool_info_list";

        if (!is_array(self::$map1[$key])) {
            self::$map1[$key] = [$poolInfo];
        } else {
            self::$map1[$key][] = $poolInfo;
        }
    }

    public static function getPoolInfo(string $poolType, int $workerId): ?PoolInfo
    {
        if ($workerId < 0) {
            return null;
        }

        $key = "{$poolType}_pool_info_list";

        if (!is_array(self::$map1[$key]) || $workerId > count(self::$map1[$key]) - 1) {
            return null;
        }

        return self::$map1[$key][$workerId];
    }

    public static function getConnection(string $connectionType)
    {
        if (Swoole::inCoroutineMode(true)) {
            $poolType = in_array($connectionType, ['pdo', 'redis']) ? $connectionType : 'unknow';
            $workerId = Swoole::getWorkerId();
            $pool = self::getPool($poolType, $workerId);
            $conn = null;

            if ($pool instanceof PoolInterface) {
                try {
                    $conn = $pool->take();
                } catch (Throwable) {
                    $conn = null;
                }
            }

            if (!is_object($conn)) {
                $conn = match ($connectionType) {
                    'pdo' => ConnectionBuilder::buildPdoConnection(),
                    'redis' => ConnectionBuilder::buildRedisConnection(),
                    default => null,
                };
            }

            return $conn;
        }

        return match ($connectionType) {
            'pdo' => ConnectionBuilder::buildPdoConnection(),
            'redis' => ConnectionBuilder::buildRedisConnection(),
            default => null,
        };
    }

    public static function getPoolIdFromConnection($conn): string
    {
        if (!is_object($conn) || !($conn instanceof ConnectionInterface)) {
            return '';
        }

        return $conn->getPoolId();
    }

    public static function isFromPool($conn): bool
    {
        return self::getPoolIdFromConnection($conn) !== '';
    }

    public static function releaseConnection($conn, ?Throwable $ex = null): void
    {
        if (!is_object($conn)) {
            return;
        }

        if (!($conn instanceof ConnectionInterface)) {
            if (method_exists($conn, 'close')) {
                try {
                    $conn->close();
                } catch (Throwable) {
                }
            }

            unset($conn);
            return;
        }

        $poolId = $conn->getPoolId();
        $poolType = StringUtils::substringBefore($poolId, ':');
        $pool = self::getPool($poolType);

        if (!($pool instanceof PoolInterface) || $pool->getPoolId() !== $poolId) {
            return;
        }

        if ($ex instanceof Throwable && stripos($ex->getMessage(), 'gone away') !== false) {
            $pool->updateCurrentActive(-1);

            if ($pool->inDebugMode()) {
                $logger = $pool->getLogger();

                if (is_object($logger)) {
                    $workerId = Swoole::getWorkerId();
                    $id = $conn->getConnectionId();
                    $logger->info("in worker$workerId, $poolType connection[$id] has gone away, remove from pool");
                }
            }

            return;
        }

        $pool->release($conn);
    }

    public static function destroyPool(string $poolType, ?int $workerId = null, $timeout = null): void
    {
        $pool = self::getPool($poolType, $workerId);

        if (!is_object($pool)) {
            return;
        }

        $pool->destroy($timeout);
    }
}
