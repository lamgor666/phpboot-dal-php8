<?php

namespace phpboot\dal\redis;

use phpboot\common\Cast;
use phpboot\common\swoole\Swoole;

final class RedisConfig
{
    /**
     * @var array
     */
    private static array $map1 = [];

    /**
     * @var bool
     */
    private bool $enabled;

    /**
     * @var string
     */
    private string $host = '127.0.0.1';

    /**
     * @var int
     */
    private int $port = 6379;

    /**
     * @var string
     */
    private string $password = '';

    /**
     * @var int
     */
    private int $database = 0;

    /**
     * @var int
     */
    private int $readTimeout = -1;

    /**
     * @var array|null
     */
    private ?array $cliSettings = null;

    private function __construct(?array $settings = null)
    {
        if (!is_array($settings)) {
            $settings = [];
        }

        $enabled = false;

        foreach ($settings as $key => $value) {
            if (!is_string($key) || $key === '') {
                continue;
            }

            $pname = strtr($key, ['-' => ' ', '_' => ' ']);
            $pname = str_replace(' ', '', ucwords($pname));
            $pname = lcfirst($pname);

            if (!property_exists($this, $pname)) {
                continue;
            }

            $enabled = true;
            $this->$pname = $value;
        }

        $this->enabled = $enabled;
    }

    public static function create(?array $settings = null): self
    {
        if (is_array($settings)) {
            if (is_string($settings['readTimeout']) && $settings['readTimeout'] !== '') {
                $settings['readTimeout'] = Cast::toDuration($settings['readTimeout']);
            }

            if (is_string($settings['read-timeout']) && $settings['read-timeout'] !== '') {
                $settings['read-timeout'] = Cast::toDuration($settings['read-timeout']);
            }

            if (is_array($settings['cliMode'])) {
                $settings['cliSettings'] = $settings['cliMode'];
                unset($settings['cliMode']);
            } else if (is_array($settings['cli-mode'])) {
                $settings['cliSettings'] = $settings['cli-mode'];
                unset($settings['cli-mode']);
            }
        }

        return new self($settings);
    }

    public static function withConfig(RedisConfig $cfg): void
    {
        $key = 'current';
        self::$map1[$key] = $cfg;
    }

    public static function loadCurrent(): ?RedisConfig
    {
        $key = 'current';
        $cfg = self::$map1[$key];
        return $cfg instanceof RedisConfig ? $cfg : null;
    }

    /**
     * @return bool
     */
    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    /**
     * @return string
     */
    public function getHost(): string
    {
        return $this->host;
    }

    /**
     * @return int
     */
    public function getPort(): int
    {
        return $this->port;
    }

    /**
     * @return string
     */
    public function getPassword(): string
    {
        return $this->password;
    }

    /**
     * @return int
     */
    public function getDatabase(): int
    {
        return $this->database;
    }

    /**
     * @return int
     */
    public function getReadTimeout(): int
    {
        return $this->readTimeout;
    }

    /**
     * @return array|null
     */
    public function getCliSettings(): ?array
    {
        return $this->cliSettings;
    }
}
