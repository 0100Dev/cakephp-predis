<?php
namespace Renan\Cake\Predis;

use Cake\Cache\CacheEngine;
use Cake\Http\Exception\NotImplementedException;
use Predis\Client;
use Predis\ClientInterface;

final class PredisEngine extends CacheEngine
{
    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * The default config used unless overridden by runtime configuration
     *
     * - `duration` Specify how long items in this cache configuration last.
     * - `prefix` Prefix appended to all entries. Good for when you need to share a keyspace
     *    with either another cache config or another application.
     * - `probability` Probability of hitting a cache gc cleanup. Setting to 0 will disable
     *    cache::gc from ever being called automatically.
     * - `connections` One or more URI strings or named arrays, eg:
     *    URI: 'tcp://127.0.0.1:6379'
     *    Named array: ['scheme' => 'tcp', 'host' => '127.0.0.1', 'port' => 6379]
     * - `options` A list of options as accepted by Predis\Client
     *
     * @var array
     */
    protected $_defaultConfig = [
        'duration' => 3600,
        'groups' => [],
        'prefix' => 'cake_',
        'probability' => 100,
        'connections' => 'tcp://127.0.0.1:6379',
        'password' => null,
        'options' => null,
    ];

    /**
     * {@inheritDoc}
     */
    public function init(array $config = []): bool
    {
        parent::init($config);

        $this->client = new Client($this->_config['connections'], $this->_config['options']);
        $this->client->connect();

        if (!is_null($this->_config['password'])) {
            $this->client->auth($this->_config['password']);
        }

        return $this->client->isConnected();
    }

    /**
     * {@inheritDoc}
     */
    public function set($key, $value, $ttl = null): bool
    {
        $ttl = $ttl ?? $this->_config['duration'];

        $key = $this->_key($key);
        $value = is_int($value)
            ? (int) $value
            : serialize($value);

        if ($ttl === 0) {
            return (string) $this->client->set($key, $value) === 'OK';
        }

        return (string) $this->client->setex($key, $ttl, $value) === 'OK';
    }

    /**
     * {@inheritDoc}
     */
    public function get($key, $default = null)
    {
        $key = $this->_key($key);
        $value = $this->client->get($key);

        if ($value === null) {
            return $default;
        }

        return ctype_digit($value)
            ? (int) $value
            : unserialize($value);
    }

    /**
     * {@inheritDoc}
     */
    public function increment($key, $offset = 1)
    {
        $key = $this->_key($key);
        if (! $this->client->exists($key)) {
            return false;
        }

        return $this->client->incrby($key, $offset);
    }

    /**
     * {@inheritDoc}
     */
    public function decrement($key, $offset = 1)
    {
        $key = $this->_key($key);
        if (! $this->client->exists($key)) {
            return false;
        }

        return $this->client->decrby($key, $offset);
    }

    /**
     * {@inheritDoc}
     */
    public function delete($key): bool
    {
        $key = $this->_key($key);
        if (! $this->client->exists($key)) {
            return false;
        }

        return $this->client->del($key) === 1;
    }

    /**
     * {@inheritDoc}
     */
    public function clear(): bool
    {
        $keys = $this->client->keys($this->_config['prefix'] . '*');
        foreach ($keys as $key) {
            $this->client->del($key);
        }

        return true;
    }

    /**
     * @inheritDoc
     */
    public function clearGroup(string $group): bool
    {
        throw new NotImplementedException();
    }
}
