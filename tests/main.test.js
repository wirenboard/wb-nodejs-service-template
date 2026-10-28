/**
 * Unit tests of the service. One test per behaviour, not per case. How the names are
 * chosen: docs/wb-service-guide.md.
 *
 * The tests import main.js, so importing it must not start anything, and main() must
 * return an exit code instead of calling process.exit. Please keep it that way.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
    DEFAULT_CONFIG_PATH,
    EXIT_CONFIG_ERROR,
    EXIT_INVALID_ARGUMENT,
    EXIT_SUCCESS,
    CONTROL,
    controlTopic,
    DEVICE,
    LOG_LEVELS,
    RESET_TOPIC,
    VERSION,
    TrafficService,
    loadConfig,
    main,
    parseArgs,
    parseRxBytes,
    shouldLog,
    speedKib,
} from '../src/main.js';

// Two lines of a real /proc/net/dev, with a shortened header.
const NET_DEV = [
    'Inter-|   Receive                                                |  Transmit',
    ' face |bytes    packets errs drop fifo frame compressed multicast|bytes',
    '    lo:    1234      10    0    0    0     0          0         0   1234',
    ' eth0: 9876543    5000    0    0    0     0          0         0  12345',
].join('\n');

const CONFIG = { interface: 'eth0', update_interval: 1, log_level: 'info' };

/**
 * Replaces the MQTT client, so the tests need no broker. It remembers what was
 * published and how the client was closed. With `offline` a publish fails the same
 * way as in a client that has no connection.
 */
function fakeClient({ offline = false } = {}) {
    const client = { sent: [], ended: null };
    client.publish = (topic, payload, options, done) => {
        client.sent.push({ topic, payload });
        if (done) {
            done(offline ? new Error('No connection to broker') : undefined);
        }
    };
    client.end = (force) => { client.ended = { force }; };
    return client;
}

function configFile(body) {
    const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'wbtpl-')), 'test.conf');
    fs.writeFileSync(file, body);
    return file;
}

test('config path defaults to the packaged one and can be overridden', () => {
    assert.equal(parseArgs(['node', 'svc']).configPath, DEFAULT_CONFIG_PATH);
    assert.equal(parseArgs(['node', 'svc', '-c', '/tmp/a.conf']).configPath, '/tmp/a.conf');
    assert.equal(parseArgs(['node', 'svc', '--config', '/tmp/b.conf']).configPath, '/tmp/b.conf');
});

test('a command line it does not understand returns invalid argument', () => {
    // Code 2 is also listed in RestartPreventExitStatus, so a typo in the systemd
    // unit does not start a restart loop.
    assert.equal(main(['node', 'svc', '--verbose']), EXIT_INVALID_ARGUMENT);
    assert.equal(main(['node', 'svc', '-c']), EXIT_INVALID_ARGUMENT);
});

test('an unusable config returns config error', () => {
    // The systemd unit lists code 6 in RestartPreventExitStatus. So every mistake in
    // the config must give exactly this code, otherwise systemd restarts the service
    // again and again.
    const cases = [
        '/nonexistent/none.conf',
        configFile('{ not json'),
        configFile('{"update_interval": 10}'),
        configFile('{"interface": "eth0", "update_interval": 0}'),
        configFile('{"interface": "eth0", "update_interval": "10"}'),
        // The schema says integer, so the file is held to the same rule.
        configFile('{"interface": "eth0", "update_interval": 0.5}'),
        // Above this setInterval overflows and fires every millisecond instead.
        configFile('{"interface": "eth0", "update_interval": 2147484}'),
        configFile('{"interface": "eth0", "update_interval": 1, "log_level": "loud"}'),
    ];
    for (const file of cases) {
        assert.equal(main(['node', 'svc', '-c', file]), EXIT_CONFIG_ERROR, file);
    }
});

test('shipped config is readable and complete', () => {
    // Reads the real file from the repository and not a temporary one. A mistake in
    // the config we ship would otherwise be found only after installing the package.
    const config = loadConfig(path.join(import.meta.dirname, '..', 'wb-nodejs-service-template.conf'));

    assert.equal(typeof config.interface, 'string');
    assert.equal(typeof config.update_interval, 'number');
    assert.ok(LOG_LEVELS.includes(config.log_level));
});

test('received bytes are taken from the right interface', () => {
    assert.equal(parseRxBytes(NET_DEV, 'eth0'), 9876543);
    assert.equal(parseRxBytes(NET_DEV, 'lo'), 1234);
    // A wrong interface name in the config must not look like zero traffic.
    assert.equal(parseRxBytes(NET_DEV, 'wlan0'), null);
});

test('speed is the difference between samples', () => {
    assert.equal(speedKib(0, 102400, 10000), 10);
    // A reboot sets the counter back to zero, on 32-bit kernels it starts from zero
    // again after the maximum, and two samples can happen in the same millisecond.
    // None of this is negative traffic.
    assert.equal(speedKib(5000, 10, 10000), 0);
    assert.equal(speedKib(0, 102400, 0), 0);
});

test('a press of the button resets the peak, other topics are ignored', () => {
    // The service is used directly, with a client that only remembers messages. The
    // handlers are methods of the class, so no broker and no event loop are needed.
    const service = new TrafficService(CONFIG);
    service.client = fakeClient();
    service.peak = 42;

    service.onMessage('/devices/other/controls/relay/on');
    assert.equal(service.peak, 42);
    assert.equal(service.client.sent.length, 0);

    service.onMessage(RESET_TOPIC);
    assert.equal(service.peak, 0);
    assert.ok(service.client.sent.some((m) => m.topic.endsWith(CONTROL.peak) && m.payload === '0'));
});

test('the error of a control is written only when it changes', () => {
    // An empty error on every update is just noise, but an error from a previous run
    // must be removed.
    const service = new TrafficService(CONFIG);
    service.client = fakeClient();
    const errors = () => service.client.sent.filter((m) => m.topic.endsWith('/meta/error'));

    service.publishDevice();
    assert.equal(errors().length, Object.keys(DEVICE.controls).length + 1);

    service.client.sent.length = 0;
    service.setRxError(true);
    service.setRxError(true);
    service.setRxError(false);
    assert.deepEqual(errors().map((m) => m.payload), ['r', '']);
});

test('a stop takes the device out of the web interface', () => {
    // The conventions say that a service stopped on purpose must leave no retained
    // topics. Otherwise the web interface keeps showing a dead device with its last
    // values. Until now this was checked only by hand on a controller.
    const service = new TrafficService(CONFIG);
    service.client = fakeClient();

    service.publishDevice();
    service.publish(controlTopic(CONTROL.peak), '7');
    const retained = new Set(service.client.sent.filter((m) => m.payload !== '').map((m) => m.topic));
    assert.ok(retained.size > 0, 'nothing was published, the test checks nothing');

    service.client.sent.length = 0;
    let wiped = 0;
    service.wipeDevice(() => { wiped += 1; });

    const emptied = service.client.sent.filter((m) => m.payload === '').map((m) => m.topic);
    for (const topic of retained) {
        assert.ok(emptied.includes(topic), `${topic} stays in the broker`);
    }
    // The device disappears from the web interface when its own meta becomes empty.
    // The stop waits for exactly this publish, and the callback comes with it.
    assert.ok(emptied.at(-1).endsWith('/meta') && !emptied.at(-1).includes('/controls/'));
    assert.equal(wiped, 1);
});

test('a stop without a broker closes the client instead of waiting', () => {
    // A mistake here costs 90 seconds on every systemctl stop and ends with SIGKILL
    // each time the broker is restarted. Without a connection the client must be
    // closed by force.
    const service = new TrafficService(CONFIG);
    service.client = fakeClient({ offline: true });

    service.onSignal('SIGTERM');

    assert.equal(service.client.ended.force, true);
    assert.equal(service.stopTimer, null, 'the stop deadline must be cleared');
});

test('the log level decides what is printed', () => {
    // A quiet service still prints errors, a detailed one prints everything.
    assert.equal(shouldLog('error', 'error'), true);
    assert.equal(shouldLog('error', 'info'), false);
    assert.equal(shouldLog('debug', 'debug'), true);
    assert.equal(shouldLog('info', 'debug'), false);
});

test('--version prints the version', (t) => {
    // The format is not checked here on purpose. The tests run before the build
    // writes the real version, so a test would always see the placeholder. The real
    // check is npm pkg set version during the build: it rejects a version npm cannot
    // use.
    const printed = [];
    t.mock.method(console, 'log', (line) => printed.push(line));

    assert.equal(main(['node', 'svc', '--version']), EXIT_SUCCESS);
    assert.deepEqual(printed, [VERSION]);
});
