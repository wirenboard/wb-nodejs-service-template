/**
 * Wiren Board service: incoming network traffic of the controller, as a virtual
 * device. How a service of ours is built: docs/wb-service-guide.md.
 *
 * Importing this module must not start anything: the tests call main() directly.
 */

import fs from 'node:fs';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import mqtt from 'mqtt';

// LSB exit codes. Codes 2 and 6 are listed in RestartPreventExitStatus, so systemd
// does not restart the service after them.
export const EXIT_SUCCESS = 0;
export const EXIT_FAILURE = 1;
export const EXIT_INVALID_ARGUMENT = 2;
export const EXIT_CONFIG_ERROR = 6;

export const DEFAULT_CONFIG_PATH = '/etc/wb-nodejs-service-template.conf';
// Without the +unix suffix mqtt.js ignores the path and connects over TCP instead
// of the socket, and it reports no error.
const BROKER_URL = 'mqtt+unix:///var/run/mosquitto/mosquitto.sock';
const DEVICE_ID = 'wb-nodejs-service-template';
const DEVICE_TOPIC = `/devices/${DEVICE_ID}`;
const PROC_NET_DEV = '/proc/net/dev';
const SIGNALS = ['SIGTERM', 'SIGINT', 'SIGHUP'];
// A stop must finish faster than this. If it does not, systemd waits for
// TimeoutStopSec and then kills the process.
const STOP_TIMEOUT_MS = 3000;
// A larger value does not fit the 32-bit delay of setInterval, and then the timer
// fires every millisecond.
const MAX_UPDATE_INTERVAL_S = 2147483;

// From the quietest level to the most detailed one.
export const LOG_LEVELS = ['error', 'info', 'debug'];
const DEFAULT_LOG_LEVEL = 'info';

// Replaced at build time from debian/changelog, see debian/rules.
export const VERSION = '0.0.0-dev';

/**
 * The whole device is described here. Publishing, cleaning and topic names are built
 * from this description, so a new control is added here and nowhere else.
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
 * Reads the command line. Throws on an unknown argument, and the caller returns
 * EXIT_INVALID_ARGUMENT.
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
    // Whole seconds, the same rule as in the schema: the file must not accept what
    // the web interface rejects.
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
 * Received byte counter of an interface, read from /proc/net/dev. Returns null if
 * there is no such interface or the line cannot be parsed. The caller then marks the
 * control with an error and the service keeps running.
 */
export function parseRxBytes(procNetDev, iface) {
    for (const line of procNetDev.split('\n')) {
        const [name, rest] = line.split(':');
        if (!rest || name.trim() !== iface) {
            continue;
        }
        const bytes = Number(rest.trim().split(/\s+/)[0]);
        // A published NaN would look like a real measurement, not like a failure.
        return Number.isFinite(bytes) ? bytes : null;
    }
    return null;
}

/** True if a message of level `level` is printed when the config asks for `configured`. */
export function shouldLog(configured, level) {
    return LOG_LEVELS.indexOf(level) <= LOG_LEVELS.indexOf(configured);
}

/**
 * Receive rate between two samples of the byte counter, in KiB/s. If the counter went
 * down (after a reboot, or when a 32-bit counter starts from zero again), the result is
 * zero and not negative traffic.
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

        // A handler is passed by reference, and a method loses its `this` on the way.
        // bind keeps it.
        this.onConnect = this.onConnect.bind(this);
        this.onMessage = this.onMessage.bind(this);
        this.onClose = this.onClose.bind(this);
        this.onError = this.onError.bind(this);
        this.onSignal = this.onSignal.bind(this);
        this.update = this.update.bind(this);
        this.onDeviceWiped = this.onDeviceWiped.bind(this);
    }

    /**
     * Every topic of a device is published with retain, so a client that connects later
     * sees the device at once and does not wait for the next update. All publishing goes
     * through this method, so the flag is set in one place.
     */
    publish(topic, payload, done) {
        this.client.publish(topic, payload, { retain: true }, done);
    }

    /** Prints the message if the configured level allows it. */
    log(level, message) {
        if (shouldLog(this.config.log_level, level)) {
            console.log(message);
        }
    }

    /** Publishes the device, its controls and empty error topics. */
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
     * Publishes an empty value to every retained topic of the device. The device then
     * disappears from the web interface instead of staying there with old values. An old
     * error would stay there too, so meta/error is cleaned as well.
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
            // A typo in the config only marks the control. The service keeps running.
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

    /** Only the button topic is handled. Everything else is ignored. */
    onMessage(topic) {
        if (topic !== RESET_TOPIC) {
            return;
        }
        this.peak = 0;
        this.publish(controlTopic(CONTROL.peak), '0');
        this.log('debug', 'peak reset from the web interface');
    }

    onClose() {
        // This also fires after a failed attempt, when there was no connection yet.
        if (!this.connected) {
            return;
        }
        this.connected = false;
        this.wasDisconnected = true;
        this.log('info', 'mqtt client disconnected');
    }

    onError(err) {
        // Errors like ECONNREFUSED keep the reason in .code, not in .message.
        this.log('error', `mqtt error: ${err.code || err.message}`);
    }

    onDeviceWiped() {
        clearTimeout(this.stopTimer);
        this.stopTimer = null;
        // Without a broker there is nothing left to send, so close the client by force
        // instead of waiting for a DISCONNECT that cannot be delivered.
        this.client.end(!this.connected, {}, exitSuccess);
    }

    onSignal(signal) {
        if (this.stopping) {
            return;
        }
        this.stopping = true;
        clearInterval(this.timer);
        this.log('info', `got ${signal}, stopping`);

        // A safety net: if a publish or the disconnect never calls back, exit anyway.
        this.stopTimer = setTimeout(exitSuccess, STOP_TIMEOUT_MS);
        this.stopTimer.unref();
        this.wipeDevice(this.onDeviceWiped);
    }

    /** Connects and keeps the device up to date until a signal arrives. */
    run() {
        this.client = mqtt.connect(BROKER_URL, {
            // Registered before connecting, so the broker reports a crash for us.
            // An error topic holds the flags r, w, p from the conventions, not a text.
            will: {
                topic: `${DEVICE_TOPIC}/meta/error`, payload: 'r', retain: true, qos: 0,
            },
            // Do not queue messages while the broker is away: after a reconnect the
            // device is published again, and a queued publish never calls back, which
            // makes the stop hang.
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
        // Only errors before the event loop start here. Later ones the client retries.
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
