<?php

namespace phpboot\dal\db;

use Closure;
use Illuminate\Support\Collection;
use phpboot\common\AppConf;
use phpboot\common\Cast;
use phpboot\common\constant\Regexp;
use phpboot\common\swoole\Swoole;
use phpboot\common\util\ExceptionUtils;
use phpboot\common\util\FileUtils;
use phpboot\common\util\JsonUtils;
use phpboot\common\util\StringUtils;
use phpboot\dal\ConnectionBuilder;
use phpboot\dal\ConnectionInterface;
use phpboot\dal\pool\PoolInterface;
use phpboot\dal\pool\PoolManager;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Throwable;

final class DB
{
    private static array $map1 = [];

    private function __construct()
    {
    }

    public static function withLogger(LoggerInterface $logger): void
    {
        $key = 'logger';
        self::$map1[$key] = $logger;
    }

    private static function getLogger(): ?LoggerInterface
    {
        $key = 'logger';
        $logger = self::$map1[$key];
        return $logger instanceof LoggerInterface ? $logger : null;
    }

    public static function debugLogEnabled(?bool $flag = null): bool
    {
        $key = 'debug_log_enabled';

        if (is_bool($flag)) {
            self::$map1[$key] = $flag;
            return false;
        }

        return self::$map1[$key] === true;
    }

    public static function withTableSchemasCacheFilepath(string $fpath): void
    {
        $key = 'table_schemas_cache_filepath';
        self::$map1[$key] = FileUtils::getRealpath($fpath);
    }
    
    private static function getTableSchemasCacheFilepath(): string
    {
        $key = 'table_schemas_cache_filepath';
        $fpath = self::$map1[$key];
        
        if (!is_string($fpath) || $fpath === '') {
            $fpath = FileUtils::getRealpath('classpath:cache/table_schemas.php');
        }
        
        return $fpath;
    }

    public static function buildTableSchemas(): void
    {
        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('datasource.forceTableSchemasCache')) {
            return;
        }

        if (Swoole::inCoroutineMode(true)) {
            $schemas = self::buildTableSchemasInternal();

            if (empty($schemas)) {
                return;
            }
            
            $workerId = Swoole::getWorkerId();
            $key = "tableSchemas_worker$workerId";
            self::$map1[$key] = $schemas;
            return;
        }

        $cacheFile = self::getTableSchemasCacheFilepath();

        if (is_file($cacheFile)) {
            try {
                $schemas = include($cacheFile);
            } catch (Throwable) {
                $schemas = [];
            }

            if (is_array($schemas) && !empty($schemas)) {
                return;
            }
        }

        self::writeTableSchemasToCacheFile(self::buildTableSchemasInternal());
    }

    public static function getTableSchemas(): array
    {
        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('datasource.forceTableSchemasCache')) {
            return self::buildTableSchemasInternal();
        }

        if (Swoole::inCoroutineMode(true)) {
            $workerId = Swoole::getWorkerId();
            $key = "tableSchemas_worker$workerId";
            $schemas = self::$map1[$key];

            if (!is_array($schemas) || empty($schemas)) {
                self::buildTableSchemas();
                $schemas = self::$map1[$key];
            }

            return is_array($schemas) ? $schemas : [];
        }

        $cacheFile = self::getTableSchemasCacheFilepath();

        if (!is_file($cacheFile)) {
            self::buildTableSchemas();
        }

        if (!is_file($cacheFile)) {
            return [];
        }

        try {
            $schemas = include($cacheFile);
        } catch (Throwable) {
            $schemas = [];
        }

        return is_array($schemas) ? $schemas : [];
    }

    public static function getTableSchema(string $tableName): array
    {
        $tableName = str_replace('`', '', $tableName);

        if (str_contains($tableName, '.')) {
            $tableName = StringUtils::substringAfterLast($tableName, '.');
        }

        $schemas = self::getTableSchemas();
        return $schemas[$tableName] ?? [];
    }

    public static function table(string $tableName): QueryBuilder
    {
        return QueryBuilder::create($tableName);
    }

    public static function raw(string $expr): Expression
    {
        return Expression::create($expr);
    }

    public static function selectBySql(string $sql, array $params = [], ?TxManager $txm = null): Collection
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return collect([]);
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return collect($stmt->fetchAll());
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function firstBySql(string $sql, array $params = [], ?TxManager $txm = null): ?array
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return null;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            $data = $stmt->fetch();
            return is_array($data) ? $data : null;
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function countBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return $stmt->fetchColumn();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function insertBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return (int) $pdo->lastInsertId();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function updateBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return $stmt->rowCount();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function sumBySql(string $sql, array $params = [], ?TxManager $txm = null): int|float|string
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            $value = $stmt->fetchColumn();

            if (is_int($value) || is_float($value)) {
                return $value;
            }

            if (!is_string($value) || $value === '') {
                return 0;
            }

            if (StringUtils::isInt($value)) {
                return Cast::toInt($value);
            }

            if (StringUtils::isFloat($value)) {
                return bcadd($value, 0, 2);
            }

            return 0;
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function deleteBySql(string $sql, array $params = [], ?TxManager $txm = null): int
    {
        return self::updateBySql($sql, $params, $txm);
    }

    public static function executeSql(string $sql, array $params = [], ?TxManager $txm = null): void
    {
        $logger = self::getLogger();
        $canWriteLog = self::debugLogEnabled() && $logger instanceof LoggerInterface;

        try {
            /* @var PDO $pdo */
            list($fromTxManager, $pdo) = self::getPdoConnection($txm);

            if ($fromTxManager && $canWriteLog) {
                $logger->info('DB Context run in transation mode');
            } else if ($pdo instanceof ConnectionInterface && $canWriteLog) {
                $logger->info('DB Context run in connection pool mode');
            }

            self::logSql($sql, $params);
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $err = null;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
        } catch (Throwable $ex) {
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            if (!$fromTxManager) {
                PoolManager::releaseConnection($pdo, $err);
            }
        }
    }

    public static function transations(Closure $callback, int|string|null $timeout = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (is_string($timeout) && $timeout !== '') {
                $timeout = Cast::toDuration($timeout);
            }

            if (!is_int($timeout) || $timeout < 1) {
                $timeout = 30;
            }

            self::transationsAsync($callback, $timeout);
            return;
        }

        $pdo = ConnectionBuilder::buildPdoConnection();

        if (!is_object($pdo)) {
            throw new DbException(null, 'fail to get database connection');
        }

        $txm = TxManager::create($pdo);
        $err = null;

        try {
            $pdo->beginTransaction();
            $callback->call($txm);
            $pdo->commit();
        } catch (Throwable $ex) {
            $pdo->rollBack();
            $err = self::wrapAsDbException($ex);
            self::writeErrorLog($err);
            throw $err;
        } finally {
            PoolManager::releaseConnection($pdo, $err);
        }
    }

    private static function transationsAsync(Closure $callback, int $timeout): void
    {
        $pdo = null;
        $pool = PoolManager::getPool('pdo');

        if ($pool instanceof PoolInterface) {
            try {
                $pdo = $pool->take();
            } catch (Throwable) {
                $pdo = null;
            }
        }

        if (!is_object($pdo)) {
            $pdo = ConnectionBuilder::buildPdoConnection();
        }

        if (!is_object($pdo) || !($pdo instanceof PDO)) {
            throw new DbException(null, 'fail to get database connection');
        }

        $txm = TxManager::create($pdo);
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        $wg = new \Swoole\Coroutine\WaitGroup();
        $wg->add();

        try {
            go(function () use ($callback, $pdo, $txm, $wg) {
                $err = null;

                try {
                    $pdo->beginTransaction();
                    $callback->call($txm);
                    $pdo->commit();
                } catch (Throwable $ex) {
                    $pdo->rollBack();
                    $err = self::wrapAsDbException($ex);
                    self::writeErrorLog($err);
                    throw $err;
                } finally {
                    PoolManager::releaseConnection($pdo, $err);
                    $wg->done();
                }
            });

            $wg->wait(floatval($timeout));
        } catch (Throwable $ex) {
            throw new DbException(null, $ex->getMessage());
        }
    }

    private static function getPdoConnection(?TxManager $txm): array
    {
        if (is_object($txm)) {
            return [true, $txm->getPdo()];
        }

        $ex = new DbException(null, "fail to get database connection");
        $pdo = PoolManager::getConnection('pdo');

        if (!is_object($pdo) || !($pdo instanceof PDO)) {
            throw $ex;
        }

        return [false, $pdo];
    }

    private static function pdoBindParams(PDOStatement $stmt, array $params): void
    {
        if (empty($params)) {
            return;
        }

        foreach ($params as $i => $value) {
            if ($value === null) {
                $stmt->bindValue($i + 1, null, PDO::PARAM_NULL);
                continue;
            }

            if (is_int($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_INT);
                continue;
            }

            if (is_float($value)) {
                $stmt->bindValue($i + 1, "$value");
                continue;
            }

            if (is_string($value)) {
                $stmt->bindValue($i + 1, $value);
                continue;
            }

            if (is_bool($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_BOOL);
                continue;
            }

            if (is_array($value)) {
                throw new DbException(null, 'fail to bind param, param type: array');
            }

            if (is_resource($value)) {
                throw new DbException(null, 'fail to bind param, param type: resource');
            }

            if (is_object($value)) {
                throw new DbException(null, 'fail to bind param, param type: ' . get_class($value));
            }
        }
    }

    private static function buildTableSchemasInternal(): array
    {
        $pdo = ConnectionBuilder::buildPdoConnection();

        if (!is_object($pdo)) {
            return [];
        }

        $tables = [];

        try {
            $stmt = $pdo->prepare('SHOW TABLES');
            $stmt->execute();
            $records = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (!is_array($records) || empty($records)) {
                unset($pdo);
                return [];
            }

            foreach ($records as $record) {
                foreach ($record as $key => $value) {
                    if (str_contains($key, 'Tables_in')) {
                        $tables[] = trim($value);
                        break;
                    }
                }
            }
        } catch (Throwable) {
            unset($pdo);
            return [];
        }

        if (empty($tables)) {
            unset($pdo);
            return [];
        }

        $schemas = [];

        foreach ($tables as $tableName) {
            try {
                $stmt = $pdo->prepare("DESC $tableName");
                $stmt->execute();
                $items = $stmt->fetchAll(PDO::FETCH_ASSOC);

                if (!is_array($items) || empty($items)) {
                    continue;
                }

                $fieldNames = [
                    'ctime',
                    'create_at',
                    'createAt',
                    'create_time',
                    'createTime',
                    'update_at',
                    'updateAt',
                    'delete_at',
                    'deleteAt',
                    'del_flag',
                    'delFlag'
                ];

                $schema = [];

                foreach ($items as $item) {
                    $fieldName = $item['Field'];

                    if (!in_array($fieldName, $fieldNames)) {
                        continue;
                    }

                    $nullable = stripos($item['Null'], 'YES') !== false;
                    $isPrimaryKey = $item['Key'] === 'PRI';
                    $defaultValue = $item['Default'];
                    $autoIncrement = $item['Extra'] === 'auto_increment';
                    $parts = preg_split(Regexp::SPACE_SEP, $item['Type']);

                    if (str_contains($parts[0], '(')) {
                        $fieldType = StringUtils::substringBefore($parts[0], '(');
                        $fieldSize = str_replace($fieldType, '', $parts[0]);
                    } else {
                        $fieldType = $parts[0];
                        $fieldSize = '';
                    }

                    if (!StringUtils::startsWith($fieldSize, '(') || !StringUtils::endsWith($fieldSize, ')')) {
                        $fieldSize = '';
                    } else {
                        $fieldSize = rtrim(ltrim($fieldSize, '('), ')');
                    }

                    if (is_numeric($fieldSize)) {
                        $fieldSize = (int) $fieldSize;
                    }

                    $unsigned = stripos($item['Type'], 'unsigned') !== false;

                    $schema[] = compact(
                        'fieldName',
                        'fieldType',
                        'fieldSize',
                        'unsigned',
                        'nullable',
                        'defaultValue',
                        'autoIncrement',
                        'isPrimaryKey'
                    );
                }
            } catch (Throwable) {
                $schema = null;
            }

            if (!is_array($schema) || empty($schema)) {
                continue;
            }

            $schemas[$tableName] = $schema;
        }

        unset($pdo);
        return $schemas;
    }

    private static function writeTableSchemasToCacheFile(array $schemas): void
    {
        if (empty($schemas)) {
            return;
        }

        $cacheFile = self::getTableSchemasCacheFilepath();
        $dir = dirname($cacheFile);
        
        if (!is_string($dir) || $dir === '') {
            return;
        }
        
        if (!is_dir($dir)) {
            mkdir($dir, 0644, true);
        }
        
        if (!is_dir($dir) || !is_writable($dir)) {
            return;
        }
        
        $cacheFile = FileUtils::getRealpath($cacheFile);
        $fp = fopen($cacheFile, 'w');

        if (!is_resource($fp)) {
            return;
        }

        $sb = [
            "<?php\n",
            'return ' . var_export($schemas, true) . ";\n"
        ];

        flock($fp, LOCK_EX);
        fwrite($fp, implode('', $sb));
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private static function wrapAsDbException(Throwable $ex): DbException
    {
        if ($ex instanceof DbException) {
            return $ex;
        }

        return new DbException(null, $ex->getMessage());
    }

    private static function logSql(string $sql, ?array $params = null): void
    {
        $logger = self::getLogger();

        if (!is_object($logger) || !self::debugLogEnabled()) {
            return;
        }

        $logger->info($sql);

        if (is_array($params) && !empty($params)) {
            $logger->debug('params: ' . JsonUtils::toJson($params));
        }
    }

    private static function writeErrorLog(string|Throwable $msg): void
    {
        $logger = self::getLogger();

        if (!is_object($logger)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        $logger->error($msg);
    }
}
