/**
 * Unit tests of the service.
 *
 * They import main.js and call its functions, which only works because the module
 * starts nothing on import and main() returns an exit code instead of calling
 * process.exit. Keep it that way.
 *
 * One test per behaviour, not per case: related cases are asserted together, so
 * the list stays readable. Test names repeat the wording of the Python template,
 * see docs/wb-service-guide.md, "Как именуем тесты".
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
    DEFAULT_CONFIG_PATH,
    EXIT_CONFIG_ERROR,
    EXIT_SUCCESS,
    CONTROL,
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

// Two lines of a real /proc/net/dev, with the header stripped down to what matters.
const NET_DEV = [
    'Inter-|   Receive                                                |  Transmit',
    ' face |bytes    packets errs drop fifo frame compressed multicast|bytes',
    '    lo:    1234      10    0    0    0     0          0         0   1234',
    ' eth0: 9876543    5000    0    0    0     0          0         0  12345',
].join('\n');

const CONFIG = { interface: 'eth0', update_interval: 1, log_level: 'info' };

// Collects what the service would publish, so the tests need no broker.
function fakeClient() {
    const sent = [];
    return { sent, publish: (topic, payload) => sent.push({ topic, payload }) };
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

test('an unusable config returns config error', () => {
    // The systemd unit lists code 6 in RestartPreventExitStatus, so every way of
    // getting the config wrong must end exactly here and not in a restart loop.
    const cases = [
        '/nonexistent/none.conf',
        configFile('{ not json'),
        configFile('{"update_interval": 10}'),
        configFile('{"interface": "eth0", "update_interval": 0}'),
        configFile('{"interface": "eth0", "update_interval": "10"}'),
        configFile('{"interface": "eth0", "update_interval": 1, "log_level": "loud"}'),
    ];
    for (const file of cases) {
        assert.equal(main(['node', 'svc', '-c', file]), EXIT_CONFIG_ERROR, file);
    }
});

test('shipped config is readable and complete', () => {
    // Reads the real file from the repository, not a temporary one: a typo in the
    // shipped config would otherwise surface only after installing the package.
    const config = loadConfig(path.join(import.meta.dirname, '..', 'wb-nodejs-service-template.conf'));

    assert.equal(typeof config.interface, 'string');
    assert.equal(typeof config.update_interval, 'number');
    assert.ok(LOG_LEVELS.includes(config.log_level));
});

test('received bytes are taken from the right interface', () => {
    assert.equal(parseRxBytes(NET_DEV, 'eth0'), 9876543);
    assert.equal(parseRxBytes(NET_DEV, 'lo'), 1234);
    // A wrong name in the config must not look like zero traffic.
    assert.equal(parseRxBytes(NET_DEV, 'wlan0'), null);
});

test('speed is the difference between samples', () => {
    assert.equal(speedKib(0, 102400, 10000), 10);
    // The counter is reset by a reboot and wraps around on 32-bit kernels, and two
    // samples can land in the same millisecond: neither is negative traffic.
    assert.equal(speedKib(5000, 10, 10000), 0);
    assert.equal(speedKib(0, 102400, 0), 0);
});

test('a press of the button resets the peak, other topics are ignored', () => {
    // The service is driven directly, with a client that only collects messages:
    // the handlers are methods, so no broker and no event loop are needed.
    const service = new TrafficService(CONFIG);
    service.client = fakeClient();
    service.peak = 42;

    assert.equal(service.onMessage('/devices/other/controls/relay/on'), false);
    assert.equal(service.peak, 42);

    assert.equal(service.onMessage(RESET_TOPIC), true);
    assert.equal(service.peak, 0);
    assert.ok(service.client.sent.some((m) => m.topic.endsWith(CONTROL.peak) && m.payload === '0'));
});

test('the error of a control is written only when it changes', () => {
    // An empty error published on every tick is noise on the bus; a stale one left
    // by a previous run has to be cleared once, when the device is published.
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

test('a stop without a broker closes the client instead of waiting', () => {
    // The broker is gone, so publishing reports an error at once and there is no
    // DISCONNECT to send: the client has to be closed by force. Getting this wrong
    // costs a 90 second systemctl stop and a SIGKILL on every broker restart.
    const service = new TrafficService(CONFIG);
    let forced = null;
    service.client = {
        publish: (topic, payload, options, done) => done && done(new Error('No connection to broker')),
        end: (force) => { forced = force; },
    };

    service.onSignal('SIGTERM');

    assert.equal(forced, true);
    assert.equal(service.stopTimer, null, 'the stop deadline must be cleared');
});

test('the log level decides what is printed', () => {
    // A quiet service still reports errors; a talkative one reports everything.
    assert.equal(shouldLog('error', 'error'), true);
    assert.equal(shouldLog('error', 'info'), false);
    assert.equal(shouldLog('debug', 'debug'), true);
    assert.equal(shouldLog('info', 'debug'), false);
});

test('version is release version and --version prints it', (t) => {
    // Neither dpkg nor npm enforces three parts, so this test is the only thing
    // that does. A build stamps the version from debian/changelog.
    assert.match(VERSION, /^\d+\.\d+\.\d+(-dev)?$/);

    const printed = [];
    t.mock.method(console, 'log', (line) => printed.push(line));

    assert.equal(main(['node', 'svc', '--version']), EXIT_SUCCESS);
    assert.deepEqual(printed, [VERSION]);
});
