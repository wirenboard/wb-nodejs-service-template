/**
 * Wiren Board service: incoming network traffic of the controller, as a virtual
 * device. How a service of ours is built: docs/wb-service-guide.md.
 *
 * Importing this module must start nothing — the tests call main() directly.
 */

import fs from 'node:fs';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import mqtt from 'mqtt';

// LSB codes, the same set as the Python template. 2 and 6 are in the unit's
// RestartPreventExitStatus: a restart would only loop.
export const EXIT_SUCCESS = 0;
export const EXIT_FAILURE = 1;
export const EXIT_INVALID_ARGUMENT = 2;
export const EXIT_CONFIG_ERROR = 6;

export const DEFAULT_CONFIG_PATH = '/etc/wb-nodejs-service-template.conf';
// Same address as wb_common's DEFAULT_BROKER_URL. Drop the +unix suffix and mqtt.js
// throws the path away and connects over TCP without a word.
const BROKER_URL = 'mqtt+unix:///var/run/mosquitto/mosquitto.sock';
const DEVICE_ID = 'wb-nodejs-service-template';
const DEVICE_TOPIC = `/devices/${DEVICE_ID}`;
const PROC_NET_DEV = '/proc/net/dev';
const SIGNALS = ['SIGTERM', 'SIGINT', 'SIGHUP'];
// A stop must never outlive this, or systemd waits out TimeoutStopSec and kills us.
const STOP_TIMEOUT_MS = 3000;
// Above this setInterval overflows its 32-bit delay and fires every millisecond.
const MAX_UPDATE_INTERVAL_S = 2147483;

// From the quietest to the most talkative.
export const LOG_LEVELS = ['error', 'info', 'debug'];
const DEFAULT_LOG_LEVEL = 'info';

// Replaced at build time from debian/changelog, see debian/rules.
export const VERSION = '0.0.0-dev';

/**
 * The whole device in one place. Publishing, wiping and topic building are derived
 * from it, so a control is added here and nowhere else.
 */
export const CONTROL = {
    rx: 'rx_speed',
    peak: 'rx_peak',
    uptime: 'uptime',
    reset: 'reset_peak',
};

export const DEVICE = {
    title: { en: 'Network traffic', ru: 'Сетевой трафик' },
    controls: {
        [CONTROL.rx]: {
            type: 'value', units: 'KiB/s', readonly: true, order: 1,
            title: { en: 'Received', ru: 'Принято' },
        },
        [CONTROL.peak]: {
            type: 'value', units: 'KiB/s', readonly: true, order: 2,
            title: { en: 'Peak received', ru: 'Пик приёма' },
        },
        [CONTROL.uptime]: {
            type: 'value', units: 's', readonly: true, order: 3,
            title: { en: 'Service uptime', ru: 'Время работы сервиса' },
        },
        [CONTROL.reset]: {
            type: 'pushbutton', order: 4,
            title: { en: 'Reset peak', ru: 'Сбросить пик' },
        },
    },
};

/** Topic of a control's value. */
export function controlTopic(control) {
    return `${DEVICE_TOPIC}/controls/${control}`;
}

/** Topic a command for a control arrives in. */
export function commandTopic(control) {
    return `${controlTopic(control)}/on`;
}

export const RESET_TOPIC = commandTopic(CONTROL.reset);

/**
 * Reads the command line. Throws on anything it does not know; the caller turns
 * that into EXIT_INVALID_ARGUMENT.
 */
export function parseArgs(argv) {
    const args = argv.slice(2);
    const result = { version: false, configPath: DEFAULT_CONFIG_PATH };

    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--version') {
            result.version = true;
        } else if (args[i] === '-c' || args[i] === '--config') {
            i += 1;
            if (args[i] === undefined) {
                throw new Error(`${args[i - 1]} needs a path to a config file`);
            }
            result.configPath = args[i];
        } else {
            throw new Error(`unknown argument ${args[i]}`);
        }
    }
    return result;
}

/** Reads and checks the config; throws, and the caller returns EXIT_CONFIG_ERROR. */
export function loadConfig(path) {
    const config = JSON.parse(fs.readFileSync(path, 'utf8'));

    if (typeof config.interface !== 'string' || config.interface === '') {
        throw new Error('interface must be the name of a network interface');
    }
    // Whole seconds, as in the schema: what the web interface refuses, the file too.
    if (!Number.isInteger(config.update_interval) || config.update_interval < 1
        || config.update_interval > MAX_UPDATE_INTERVAL_S) {
        throw new Error(`update_interval must be a whole number of seconds between 1 and ${MAX_UPDATE_INTERVAL_S}`);
    }
    // Optional, and quiet by default.
    config.log_level = config.log_level ?? DEFAULT_LOG_LEVEL;
    if (!LOG_LEVELS.includes(config.log_level)) {
        throw new Error(`log_level must be one of ${LOG_LEVELS.join(', ')}`);
    }
    return config;
}

/**
 * Received byte counter of an interface, out of /proc/net/dev. null when there is
 * no such interface or the line makes no sense: that marks the control, not kills
 * the service.
 */
export function parseRxBytes(procNetDev, iface) {
    for (const line of procNetDev.split('\n')) {
        const [name, rest] = line.split(':');
        if (!rest || name.trim() !== iface) {
            continue;
        }
        const bytes = Number(rest.trim().split(/\s+/)[0]);
        // NaN published as a speed would read as a measurement, not as a failure.
        return Number.isFinite(bytes) ? bytes : null;
    }
    return null;
}

/** True when a message of this level is worth printing at the configured one. */
export function shouldLog(configured, level) {
    return LOG_LEVELS.indexOf(level) <= LOG_LEVELS.indexOf(configured);
}

/**
 * Receive rate between two samples of the byte counter, in KiB/s. A counter that
 * went backwards (reboot, 32-bit wrap) reports zero, not negative traffic.
 */
export function speedKib(prevBytes, bytes, elapsedMs) {
    if (elapsedMs <= 0 || bytes < prevBytes) {
        return 0;
    }
    return Math.round(((bytes - prevBytes) / 1024 / (elapsedMs / 1000)) * 10) / 10;
}

function exitSuccess() {
    process.exit(EXIT_SUCCESS);
}

/** The service: the client, the state kept between measurements, the handlers. */
export class TrafficService {
    constructor(config) {
        this.config = config;
        this.client = null;
        this.timer = null;
        this.stopTimer = null;

        this.prevBytes = null;
        this.prevAt = 0;
        this.peak = 0;
        this.startedAt = Date.now();

        this.stopping = false;
        this.connected = false;
        this.wasDisconnected = false;
        this.rxError = false;

        // Handlers travel by reference, and a method loses its this on the way.
        this.onConnect = this.onConnect.bind(this);
        this.onMessage = this.onMessage.bind(this);
        this.onClose = this.onClose.bind(this);
        this.onError = this.onError.bind(this);
        this.onSignal = this.onSignal.bind(this);
        this.update = this.update.bind(this);
        this.onDeviceWiped = this.onDeviceWiped.bind(this);
    }

    /**
     * Every topic of a device is retained, so that a reader that connects later sees
     * the device instead of waiting for the next tick. Hence the single entry point.
     */
    publish(topic, payload, done) {
        this.client.publish(topic, payload, { retain: true }, done);
    }

    /** Prints a message when the configured level is talkative enough. */
    log(level, message) {
        if (shouldLog(this.config.log_level, level)) {
            console.log(message);
        }
    }

    /** Publishes the device, its controls and a clean slate of errors. */
    publishDevice() {
        this.publish(`${DEVICE_TOPIC}/meta`, JSON.stringify({ driver: DEVICE_ID, title: DEVICE.title }));
        this.publish(`${DEVICE_TOPIC}/meta/error`, '');

        for (const [control, meta] of Object.entries(DEVICE.controls)) {
            this.publish(`${controlTopic(control)}/meta`, JSON.stringify(meta));
            this.publish(`${controlTopic(control)}/meta/error`, '');
        }
        this.rxError = false;
    }

    /**
     * Empties every retained topic of the device, so it leaves the web interface
     * instead of hanging there with stale values. An error left behind would hang
     * there too, hence meta/error.
     */
    wipeDevice(done) {
        for (const control of Object.keys(DEVICE.controls)) {
            this.publish(controlTopic(control), '');
            this.publish(`${controlTopic(control)}/meta`, '');
            this.publish(`${controlTopic(control)}/meta/error`, '');
        }
        this.publish(`${DEVICE_TOPIC}/meta`, '', done);
    }

    /** Measures the rate and publishes it. The work of the service. */
    update() {
        const now = Date.now();
        let rx;

        try {
            rx = parseRxBytes(fs.readFileSync(PROC_NET_DEV, 'utf8'), this.config.interface);
        } catch (err) {
            rx = null;
            this.log('error', `cannot read ${PROC_NET_DEV}: ${err.code || err.message}`);
        }

        this.publish(controlTopic(CONTROL.uptime), String(Math.round((now - this.startedAt) / 1000)));

        if (rx === null) {
            // A typo in the config marks the control; it does not kill the service.
            this.log('error', `no interface ${this.config.interface} in ${PROC_NET_DEV}`);
            this.setRxError(true);
            return;
        }
        this.setRxError(false);

        if (this.prevBytes !== null) {
            const speed = speedKib(this.prevBytes, rx, now - this.prevAt);

            this.peak = Math.max(this.peak, speed);
            this.publish(controlTopic(CONTROL.rx), String(speed));
            this.publish(controlTopic(CONTROL.peak), String(this.peak));

            this.log('debug', `${this.config.interface}: ${speed} KiB/s, peak ${this.peak} KiB/s`);
        }
        this.prevBytes = rx;
        this.prevAt = now;
    }

    /** Writes the error of the rate control, only when it changes. */
    setRxError(failed) {
        if (failed === this.rxError) {
            return;
        }
        this.rxError = failed;
        this.publish(`${controlTopic(CONTROL.rx)}/meta/error`, failed ? 'r' : '');
    }

    onConnect() {
        this.connected = true;
        if (this.wasDisconnected) {
            // The broker drops the session of a client it lost, so publish again.
            this.log('info', 'reconnected, republishing the device');
        } else {
            this.log('info', `connected to the broker, watching ${this.config.interface}`
                + ` every ${this.config.update_interval}s`);
        }

        this.publishDevice();
        this.update();

        clearInterval(this.timer);
        this.timer = setInterval(this.update, this.config.update_interval * 1000);

        this.client.subscribe(RESET_TOPIC);
    }

    /** Only the button is listened to; a service ignores what it did not ask for. */
    onMessage(topic) {
        if (topic !== RESET_TOPIC) {
            return;
        }
        this.peak = 0;
        this.publish(controlTopic(CONTROL.peak), '0');
        this.log('debug', 'peak reset from the web interface');
    }

    onClose() {
        // Fires on a failed attempt too, when there was nothing to lose yet.
        if (!this.connected) {
            return;
        }
        this.connected = false;
        this.wasDisconnected = true;
        this.log('info', 'mqtt client disconnected');
    }

    onError(err) {
        // ECONNREFUSED and friends carry the reason in .code, not in .message.
        this.log('error', `mqtt error: ${err.code || err.message}`);
    }

    onDeviceWiped() {
        clearTimeout(this.stopTimer);
        this.stopTimer = null;
        // Nothing to flush without a broker: close hard rather than wait for a
        // DISCONNECT that cannot be sent.
        this.client.end(!this.connected, {}, exitSuccess);
    }

    onSignal(signal) {
        if (this.stopping) {
            return;
        }
        this.stopping = true;
        clearInterval(this.timer);
        this.log('info', `got ${signal}, stopping`);

        // Belt and braces, should a publish or the disconnect never report back.
        this.stopTimer = setTimeout(exitSuccess, STOP_TIMEOUT_MS);
        this.stopTimer.unref();
        this.wipeDevice(this.onDeviceWiped);
    }

    /** Connects and keeps the device up to date until a signal arrives. */
    run() {
        this.client = mqtt.connect(BROKER_URL, {
            // Registered before connecting, so the broker reports a crash itself.
            // An error topic carries the conventions' flags r, w, p, not prose.
            will: {
                topic: `${DEVICE_TOPIC}/meta/error`, payload: 'r', retain: true, qos: 0,
            },
            // Never queue what the broker missed: a reconnect republishes anyway,
            // and a queued publish never calls back, which hangs the stop.
            queueQoSZero: false,
        });

        this.client.on('connect', this.onConnect);
        this.client.on('message', this.onMessage);
        this.client.on('close', this.onClose);
        this.client.on('error', this.onError);

        // Node passes the signal name to the listener, so one handler covers all.
        for (const signal of SIGNALS) {
            process.on(signal, this.onSignal);
        }
        return this.client;
    }
}

export function main(argv) {
    let args;
    try {
        args = parseArgs(argv);
    } catch (err) {
        console.log(`${err.message}
usage: ${DEVICE_ID} [--version] [-c <config>]`);
        return EXIT_INVALID_ARGUMENT;
    }

    if (args.version) {
        console.log(VERSION);
        return EXIT_SUCCESS;
    }

    let config;
    try {
        config = loadConfig(args.configPath);
    } catch (err) {
        console.log(`cannot read the config ${args.configPath}: ${err.message}`);
        return EXIT_CONFIG_ERROR;
    }

    try {
        new TrafficService(config).run();
    } catch (err) {
        // Only what breaks before the event loop: the rest the client retries.
        console.log(`cannot start: ${err.message}`);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// Runs only when the file is started directly, not when it is imported.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    const code = main(process.argv);
    if (code !== EXIT_SUCCESS) {
        process.exit(code);
    }
}
