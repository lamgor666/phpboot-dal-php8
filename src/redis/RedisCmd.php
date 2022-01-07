<?php

namespace phpboot\dal\redis;

use phpboot\common\Cast;
use phpboot\dal\pool\PoolManager;
use phpboot\common\util\StringUtils;
use Redis;
use RuntimeException;
use Throwable;

/**
 * redis command like php redis
 * see https://github.com/phpredis/phpredis for detail information
 */
final class RedisCmd
{
    private function __construct()
    {
    }

    public static function loadScript(string $scriptName): string
    {
        $filepath = str_replace("\\", '/', __DIR__ . "/scripts/$scriptName.lua");

        if (!is_file($filepath)) {
            return '';
        }

        $contents = file_get_contents($filepath);
        return is_string($contents) ? $contents : '';
    }

    public static function ping(): bool
    {
        return self::boolResult('PING');
    }

    public static function info(): array
    {
        return self::arrayResult('INFO');
    }

    /* Strings */
    public static function decr(string $key): int
    {
        return self::intResult('DECR', [$key]);
    }

    public static function decrBy(string $key, int $value): int
    {
        return self::intResult('DECRBY', [$key, "$value"]);
    }

    public static function get(string $key): string
    {
        return self::stringResult('GET', [$key]);
    }

    public static function incr(string $key): int
    {
        return self::intResult('INCR', [$key]);
    }

    public static function incrBy(string $key, int $value): int
    {
        return self::intResult('INCRBY', [$key, "$value"]);
    }

    public static function incrByFloat(string $key, float $value): float
    {
        return self::floatResult("INCRBYFLOAT", [$key, "$value"]);
    }

    /**
     * @param string[] $keys
     * @return string[]
     */
    public static function mGet(array $keys): array
    {
        return self::arrayResult('MGET', $keys);
    }

    public static function mSet(array $pairs): bool
    {
        $args = [];

        foreach ($pairs as $key => $val) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            array_push($args, $key, Cast::toString($val));
        }

        if (empty($args)) {
            return false;
        }

        return self::boolResult('MSET', $args);
    }

    public static function set(string $key, string $value): bool
    {
        return self::boolResult('SET', [$key, $value]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int|string $ttl
     * @return bool
     */
    public static function setex(string $key, string $value, $ttl): bool
    {
        if (is_string($ttl)) {
            $ttl = StringUtils::toDuration($ttl);
        }

        return self::boolResult('SETEX', [$key, "$ttl", $value]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int|string $ttl
     * @return bool
     */
    public static function psetex(string $key, string $value, $ttl): bool
    {
        if (is_string($ttl)) {
            $ttl = StringUtils::toDuration($ttl);
        }

        $ttl *= 1000;
        
        return self::boolResult('PSETEX', [$key, "$ttl", $value]);
    }

    public static function setnx(string $key, string $value): bool
    {
        return self::intResult('SETNX', [$key, $value], -1) === 0;
    }
    /* end of Strings */

    /* Keys */
    public static function del(string $key): int
    {
        return self::intResult('DEL', [$key]);
    }

    public static function exists(string $key): bool
    {
        return self::boolResult('EXISTS', [$key]);
    }

    /**
     * @param string $key
     * @param int|string $duration
     * @return bool
     */
    public static function expire(string $key, $duration): bool
    {
        if (is_string($duration)) {
            $duration = StringUtils::toDuration($duration);
        }

        return self::intResult('EXPIRE', [$key, "$duration"]) === 1;
    }

    public static function expireAt(string $key, int $timestamp): bool
    {
        return self::intResult('EXPIREAT', [$key, "$timestamp"]) === 1;
    }

    /**
     * @param string $pattern
     * @return string[]
     */
    public static function keys(string $pattern): array
    {
        return self::arrayResult('KEYS', [$pattern]);
    }

    public static function rename(string $key, string $newKey): bool
    {
        return self::boolResult('RENAME', [$key, $newKey]);
    }

    public static function renameNx(string $key, string $newKey): bool
    {
        return self::intResult('RENAMENX', [$key, $newKey]) === 1;
    }

    public static function ttl(string $key): int
    {
        return self::intResult('TTL', [$key]);
    }
    /* end of Keys */

    /* Hashes */
    /**
     * @param string $key
     * @param string[] $fields
     * @return int
     */
    public static function hDel(string $key, array $fields): int
    {
        $args = [];

        foreach ($fields as $value) {
            if (!is_string($value) || $value === '') {
                continue;
            }

            $args[] = $value;
        }

        if (empty($args)) {
            return 0;
        }

        return self::intResult('HDEL', array_merge([$key], $args));
    }

    /**
     * @param string $key
     * @return string[]
     */
    public static function hKeys(string $key): array
    {
        return self::arrayResult('HKEYS', [$key]);
    }

    /**
     * @param string $key
     * @return string[]
     */
    public static function hVals(string $key): array
    {
        return self::arrayResult('HVALS', [$key]);
    }

    public static function hGetAll(string $key): array
    {
        $entries = self::arrayResult('HGETALL', [$key]);
        $cnt = count($entries);

        if ($cnt < 1) {
            return [];
        }

        $map1 = [];

        for ($i = 0; $i < $cnt; $i += 2) {
            if ($i + 1 > $cnt - 1) {
                break;
            }

            $key = $entries[$i];
            $value = $entries[$i + 1];

            if (empty($key)) {
                continue;
            }

            $map1[$key] = $value;
        }

        return $map1;
    }

    public static function hExists(string $key, string $fieldName): bool
    {
        return self::intResult('HEXISTS', [$key, $fieldName]) === 1;
    }

    public static function hIncrBy(string $key, string $fieldName, int $num): int
    {
        return self::intResult('HINCRBY', [$key, $fieldName, "$num"]);
    }

    public static function hIncrByFloat(string $key, string $fieldName, float $num): float
    {
        return self::floatResult("HINCRBYFLOAT", [$key, $fieldName, "$num"]);
    }

    public static function hMSet(array $pairs): bool
    {
        $args = [];

        foreach ($pairs as $key => $val) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            array_push($args, $key, Cast::toString($val));
        }

        if (empty($args)) {
            return false;
        }

        return self::boolResult('HMSET', $args);
    }

    public static function hMGet(string $key, array $fieldNames): array
    {
        $args = [];

        foreach ($fieldNames as $val) {
            if (!is_string($val) || $val === '') {
                continue;
            }

            $args[] = $val;
        }

        $cnt = count($args);

        if ($cnt < 1) {
            return [];
        }

        $entries = self::arrayResult('HMGET', array_merge([$key], $args));

        if (count($entries) !== $cnt) {
            return [];
        }

        $map1 = [];

        for ($i = 0; $i < $cnt; $i++) {
            $map1[$args[$i]] = $entries[$i];
        }

        return $map1;
    }
    /* end of Hashes */

    /* Lists */
    /**
     * @param string[] $keys
     * @param int|string $timeout
     * @return string[]
     */
    public static function blPop(array $keys, $timeout): array
    {
        $args = [];

        foreach ($keys as $key) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $args[] = $key;
        }

        if (empty($args)) {
            return [];
        }

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        $args[] = "$timeout";
        return self::arrayResult('BLPOP', $args);
    }

    /**
     * @param string[] $keys
     * @param int|string $timeout
     * @return string[]
     */
    public static function brPop(array $keys, $timeout): array
    {
        $args = [];

        foreach ($keys as $key) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $args[] = $key;
        }

        if (empty($args)) {
            return [];
        }

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        $args[] = "$timeout";
        return self::arrayResult('BRPOP', $args);
    }

    /**
     * @param string $srcKey
     * @param string $dstKey
     * @param int|string $timeout
     * @return string
     */
    public static function bRPopLPush(string $srcKey, string $dstKey, $timeout): string
    {
        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        return self::stringResult('BRPOPLPUSH', [$srcKey, $dstKey, "$timeout"]);
    }

    public static function lIndex(string $key, int $idx): string
    {
        return self::stringResult('LINDEX', [$key, "$idx"]);
    }

    public static function lGet(string $key, int $idx): string
    {
        return self::lIndex($key, $idx);
    }

    public static function lInsert(string $key, string $serachValue, string $element, bool $before = false): int
    {
        $pos = $before ? 'BEFORE' : 'AFTER';
        return self::intResult('LINSERT', [$key, $pos, $serachValue, $element], -1);
    }

    /**
     * @param string $key
     * @param int|null $count
     * @return string|string[]
     */
    public static function lPop(string $key, ?int $count = null)
    {
        $cmd = 'LPOP';
        $multi = false;
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $cmd .= '@array';
            $multi = true;
            $args[] = "$count";
        }

        if ($multi) {
            return self::arrayResult($cmd, $args);
        }

        return self::stringResult($cmd, $args);
    }

    public static function lPush(string $key, string... $elements): int
    {
        return self::intResult('LPUSH', array_merge([$key], $elements));
    }

    public static function lPushx(string $key, string... $elements): int
    {
        return self::intResult('LPUSHX', array_merge([$key], $elements));
    }

    public static function lRange(string $key, int $start, int $stop): array
    {
        return self::arrayResult('LRANGE', [$key, "$start", "$stop"]);
    }

    public static function lGetRange(string $key, int $start, int $stop): array
    {
        return self::lRange($key, $start, $stop);
    }

    public static function lRem(string $key, int $count, string $element): int
    {
        return self::intResult('LREM', [$key, "$count", $element]);
    }

    public static function lRemove(string $key, int $count, string $element): int
    {
        return self::lRem($key, $count, $element);
    }

    public static function lSet(string $key, int $idx, string $value): bool
    {
        return self::boolResult('LSET', [$key, "$idx", $value]);
    }

    public static function lTrim(string $key, int $start, int $stop): bool
    {
        return self::boolResult('LTRIM', [$key, "$start", "$stop"]);
    }

    public static function listTrim(string $key, int $start, int $stop): bool
    {
        return self::lTrim($key, $start, $stop);
    }

    /**
     * @param string $key
     * @param int|null $count
     * @return string|string[]
     */
    public static function rPop(string $key, ?int $count = null)
    {
        $cmd = 'RPOP';
        $multi = false;
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $cmd .= '@array';
            $multi = true;
            $args[] = "$count";
        }

        if ($multi) {
            return self::arrayResult($cmd, $args);
        }

        return self::stringResult($cmd, $args);
    }

    public static function rPopLPush(string $srcKey, string $dstKey): string
    {
        return self::stringResult('RPOPLPUSH', [$srcKey, $dstKey]);
    }

    public static function rPush(string $key, string... $elements): int
    {
        return self::intResult('RPUSH', array_merge([$key], $elements));
    }

    public static function rPushx(string $key, string... $elements): int
    {
        return self::intResult('RPUSHX', array_merge([$key], $elements));
    }

    public static function lLen(string $key): int
    {
        return self::intResult('LLEN', [$key]);
    }

    public static function lSize(string $key): int
    {
        return self::lLen($key);
    }
    /* end of Lists */

    /* Sets */
    public static function sAdd(string $key, string... $members): int
    {
        return self::intResult('SADD', array_merge([$key], $members));
    }

    public static function sCard(string $key): int
    {
        return self::intResult('SCARD', [$key]);
    }

    public static function sSize(string $key): int
    {
        return self::sCard($key);
    }

    public static function sDiff(string $key, string... $otherKeys): array
    {
        return self::arrayResult('SDIFF', array_merge([$key], $otherKeys));
    }

    public static function sDiffStore(string $dstKey, string... $srcKeys): int
    {
        return self::intResult('SDIFFSTORE', array_merge([$dstKey], $srcKeys));
    }

    public static function sInter(string $key, string... $otherKeys): array
    {
        return self::arrayResult('SINTER', array_merge([$key], $otherKeys));
    }

    public static function sInterStore(string $dstKey, string... $srcKeys): int
    {
        return self::intResult('SINTERSTORE', array_merge([$dstKey], $srcKeys));
    }

    public static function sIsMember(string $key, string $searchValue): bool
    {
        return self::intResult('SISMEMBER', [$key, $searchValue]) === 1;
    }

    public static function sContains(string $key, string $searchValue): bool
    {
        return self::sIsMember($key, $searchValue);
    }

    public static function sMembers(string $key): array
    {
        return self::arrayResult('SMEMBERS', [$key]);
    }

    public static function sGetMembers(string $key): array
    {
        return self::sMembers($key);
    }

    public static function sMove(string $srcKey, string $dstKey, string $searchValue): bool
    {
        return self::intResult('SMOVE', [$srcKey, $dstKey, $searchValue]) === 1;
    }

    public static function sPop(string $key, ?int $count = null): array
    {
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        return self::arrayResult('SPOP', $args);
    }

    public static function sRandMember(string $key, ?int $count = null): array
    {
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        return self::arrayResult('SRANDMEMBER', $args);
    }

    public static function sRem(string $key): int
    {
        return self::intResult('SREM', [$key]);
    }

    public static function sRemove(string $key): int
    {
        return self::sRem($key);
    }

    public static function sUnion(string $key, string... $otherKeys): array
    {
        return self::arrayResult('SUNION', array_merge([$key], $otherKeys));
    }

    public static function sUnionStore(string $dstKey, string... $srcKeys): int
    {
        return self::intResult('SUNIONSTORE', array_merge([$dstKey], $srcKeys));
    }
    /* end of Sets */

    /* Sorted Sets */
    /**
     * @param array $keys
     * @param int|string $timeout
     * @param bool $max
     * @return array
     */
    public static function bzPop(array $keys, $timeout, bool $max = false): array
    {
        $cmd = $max ? 'BZPOPMAX@array' : 'BZPOPMIN@array';

        if (is_string($timeout)) {
            $timeout = StringUtils::toDuration($timeout);
        }

        return self::arrayResult($cmd, array_merge($keys, ["$timeout"]));
    }

    /**
     * @param string $key
     * @param int|float $score
     * @param string $value
     * @param array $options
     * @param mixed ...$otherScoreAndValues
     * @return bool
     */
    public static function zAdd(string $key, $score, string $value, array $options = [], ...$otherScoreAndValues): bool
    {
        $args = [$key];
        $supportedOptions = ['XX', 'NX', 'LT', 'GT', 'CH', 'INCR'];

        foreach ($options as $opt) {
            if (!is_string($opt) || $opt === '' || !in_array($opt, $supportedOptions)) {
                continue;
            }

            $args[] = $opt;
        }

        array_push($args, "$score", $value);

        if (!empty($otherScoreAndValues)) {
            $cnt = count($otherScoreAndValues);

            for ($i = 0; $i < $cnt; $i += 2) {
                if ($i + 1 > $cnt - 1) {
                    break;
                }

                $otherScore = $otherScoreAndValues[$i];

                if (!is_int($otherScore)) {
                    continue;
                }

                $otherValue = Cast::toString($otherScoreAndValues[$i + 1]);

                if ($otherValue === '') {
                    continue;
                }

                array_push($args, "$otherScore", $otherValue);
            }
        }

        return self::intResult('ZADD', $args, -1) >= 0;
    }

    public static function zCard(string $key): int
    {
        return self::intResult('ZCARD', [$key]);
    }

    public static function zSize(string $key): int
    {
        return self::zCard($key);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zCount(string $key, $start, $end): int
    {
        return self::intResult('ZCOUNT', [$key, "$start", "$end"]);
    }

    public static function zIncrBy(string $key, string $searchValue, int $num): int
    {
        return self::intResult('ZINCRBY', [$key, "$num", $searchValue]);
    }

    public static function zPop(string $key, ?int $count = null, bool $max = false): array
    {
        $cmd = $max ? 'ZPOPMAX@array' : 'ZPOPMIN@array';
        $args = [$key];

        if (is_int($count) && $count > 0) {
            $args[] = "$count";
        }

        return self::arrayResult($cmd, $args);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRange(string $key, $min, $max, array $options = []): array
    {
        $args = [$key, "$min", "$max", 'BYSCORE'];

        if (Cast::toBoolean('rev')) {
            $args[] = 'REV';
        }

        if (is_array($options['limit'])) {
            list($offset, $count) = $options['limit'];
            $offset = Cast::toInt($offset);
            $count = Cast::toInt($count);

            if ($offset >= 0 && $count > 0) {
                array_push($args, 'LIMIT', "$offset", "$count");
            }
        }

        $withscores = Cast::toBoolean($options['withscores']);

        if ($withscores) {
            $args[] = 'WITHSCORES';
        }

        $entries = self::arrayResult('ZRANGE', $args);

        if (!$withscores) {
            return $entries;
        }

        $cnt = count($entries);
        $list = [];

        for ($i = 0; $i < $cnt; $i += 2) {
            if ($i + 1 > $cnt - 1) {
                break;
            }

            $list[] = [
                'score' => $entries[$i + 1],
                'element' => $entries[$i]
            ];
        }

        return $list;
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRangeByScore(string $key, $min, $max, array $options = []): array
    {
        return self::zRange($key, $min, $max, $options);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRevRangeByScore(string $key, $min, $max, array $options = []): array
    {
        return self::zRange($key, $min, $max, $options);
    }

    public static function zRank(string $key, string $searchValue): int
    {
        return self::intResult('ZRANK', [$key, $searchValue]);
    }

    public static function zRevRank(string $key, string $searchValue): int
    {
        return self::zRank($key, $searchValue);
    }

    public static function zRem(string $key, string... $members): int
    {
        return self::intResult('ZREM', array_merge([$key], $members));
    }

    public static function zDelete(string $key, string... $members): int
    {
        return self::zRem($key, ...$members);
    }

    public static function zRemove(string $key, string... $members): int
    {
        return self::zRem($key, ...$members);
    }

    public static function zRemRangeByRank(string $key, int $start, int $end): int
    {
        return self::intResult('ZREMRANGEBYRANK', [$key, "$start", "$end"]);
    }

    public static function zDeleteRangeByRank(string $key, int $start, int $end): int
    {
        return self::zRemRangeByRank($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zRemRangeByScore(string $key, $start, $end): int
    {
        return self::intResult('ZREMRANGEBYSCORE', [$key, "$start", "$end"]);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zDeleteRangeByScore(string $key, $start, $end): int
    {
        return self::zRemRangeByScore($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $start
     * @param int|float $end
     * @return int
     */
    public static function zRemoveRangeByScore(string $key, $start, $end): int
    {
        return self::zRemRangeByScore($key, $start, $end);
    }

    /**
     * @param string $key
     * @param int|float $min
     * @param int|float $max
     * @param array $options
     * @return array
     */
    public static function zRevRange(string $key, $min, $max, array $options = []): array
    {
        $options['rev'] = true;
        return self::zRange($key, $min, $max, $options);
    }

    public static function zScore(string $key, string $searchValue): float
    {
        return self::floatResult('ZSCORE', [$key, $searchValue]);
    }
    /* end of Sorted Sets */

    private static function stringResult(string $cmd, ?array $args = null): string
    {
        try {
            $result = self::getResult($cmd, $args);
            return is_string($result) ? $result : '';
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    private static function boolResult(string $cmd, ?array $args = null): bool
    {
        try {
            $result = self::getResult($cmd, $args);

            if (is_string($result) && strtoupper($result) === 'OK') {
                return true;
            }

            return Cast::toBoolean($result);
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    public static function intResult(string $cmd, ?array $args = null, int $default = 0): int
    {
        try {
            $result = self::getResult($cmd, $args);
            return Cast::toInt($result, $default);
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    public static function floatResult(string $cmd, ?array $args = null, float $default = 0.0): float
    {
        try {
            $result = self::getResult($cmd, $args);
            return Cast::toFloat($result, $default);
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    public static function arrayResult(string $cmd, ?array $args = null): array
    {
        try {
            $result = self::getResult($cmd, $args);
            return is_array($result) ? $result : [];
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        }
    }

    private static function getResult(string $cmd, ?array $args = null)
    {
        $ex1 = new RuntimeException('RedisCmd: fail to get redis connection');
        $redis = PoolManager::getConnection('redis');

        if (!is_object($redis) || !($redis instanceof Redis)) {
            throw $ex1;
        }

        if (strpos($cmd, '@') !== false) {
            $cmd = StringUtils::substringBefore($cmd, '@');
        }

        $cmd = strtolower($cmd);

        try {
            if (empty($args)) {
                $result = call_user_func([$redis, $cmd]);
            } else {
                $result = call_user_func_array([$redis, $cmd], $args);
            }

            return $result;
        } catch (Throwable $ex) {
            throw new RuntimeException($ex->getMessage());
        } finally {
            PoolManager::releaseConnection($redis);
        }
    }
}
