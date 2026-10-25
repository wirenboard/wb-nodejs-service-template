/**
 * Wiren Board service: incoming network traffic of the controller.
 *
 * Publishes a virtual device with the current receive rate of an interface, the
 * peak rate since the last reset and the service uptime, and refreshes them on
 * the interval from the config.
 *
 * The module is import-safe on purpose: importing it must not read the config,
 * open sockets or start anything. Only the guard at the bottom runs the service,
 * so tests can call main() directly and check the exit code.
 */

import fs from 'node:fs';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import mqtt from 'mqtt';

// The same set as in the Python template: a service of Wiren Board answers with
// the same codes whatever it is written in.
export const EXIT_SUCCESS = 0;
export const EXIT_FAILURE = 1;
// The systemd unit lists this code in RestartPreventExitStatus: a broken config
// must not send the service into a restart loop.
export const EXIT_CONFIG_ERROR = 6;

export const DEFAULT_CONFIG_PATH = '/etc/wb-nodejs-service-template.conf';
const MQTT_URL = 'mqtt://localhost:1883';
const DEVICE_ID = 'wb-nodejs-service-template';
const DEVICE_TOPIC = `/devices/${DEVICE_ID}`;
const PROC_NET_DEV = '/proc/net/dev';
const SIGNALS = ['SIGTERM', 'SIGINT', 'SIGHUP'];
// A stop must never outlive this, whatever the broker is doing. Without it systemd
// waits out TimeoutStopSec (90 s by default) and then kills the service.
const STOP_TIMEOUT_MS = 3000;
// setInterval keeps its delay in a signed 32-bit integer of milliseconds. A larger
// one silently becomes 1 ms, and the service hammers the interface every tick.
const MAX_UPDATE_INTERVAL_S = 2147483;

// From the quietest to the most talkative. A level lets the user up the detail
// without a rebuild, and keeps the journal readable the rest of the time.
export const LOG_LEVELS = ['error', 'info', 'debug'];
const DEFAULT_LOG_LEVEL = 'info';

// Replaced at build time from debian/changelog, see debian/rules.
export const VERSION = '0.0.0-dev';

/**
 * The whole virtual device in one place: its title and every control with the
 * metadata the web interface needs. Publishing, wiping on stop and building
 * topics are derived from this, so a new control is added here only.
 *
 * reset_peak is the one writable control: pressed in the web interface, it drops
 * the peak back to zero. Commands arrive in the control's /on topic.
 */
export const CONTROL = {
    rx: 'rx_speed',
    peak: 'rx_peak',
    uptime: 'uptime',
    reset: 'reset_peak',
};

export const DEVICE = {
    id: DEVICE_ID,
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

export function parseArgs(argv) {
    const args = argv.slice(2);
    const result = { version: false, configPath: DEFAULT_CONFIG_PATH };

    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--version') {
            result.version = true;
        } else if (args[i] === '-c' || args[i] === '--config') {
            i += 1;
            result.configPath = args[i];
        }
    }
    return result;
}

/**
 * Reads and checks the config. Throws on a missing file, broken JSON or a bad
 * value; the caller turns that into EXIT_CONFIG_ERROR.
 */
export function loadConfig(path) {
    const config = JSON.parse(fs.readFileSync(path, 'utf8'));

    if (typeof config.interface !== 'string' || config.interface === '') {
        throw new Error('interface must be the name of a network interface');
    }
    if (typeof config.update_interval !== 'number' || config.update_interval <= 0
        || config.update_interval > MAX_UPDATE_INTERVAL_S) {
        throw new Error(`update_interval must be a number of seconds between 1 and ${MAX_UPDATE_INTERVAL_S}`);
    }
    // Optional: the quiet default suits a service nobody is debugging right now.
    config.log_level = config.log_level ?? DEFAULT_LOG_LEVEL;
    if (!LOG_LEVELS.includes(config.log_level)) {
        throw new Error(`log_level must be one of ${LOG_LEVELS.join(', ')}`);
    }
    return config;
}

/**
 * Pulls the received byte counter of an interface out of /proc/net/dev.
 * Returns null when there is no such interface — a wrong name in the config is
 * worth an error on the control, not a dead service.
 */
export function parseRxBytes(procNetDev, iface) {
    for (const line of procNetDev.split('\n')) {
        const [name, rest] = line.split(':');
        if (!rest || name.trim() !== iface) {
            continue;
        }
        return Number(rest.trim().split(/\s+/)[0]);
    }
    return null;
}

/** True when a message of this level is worth printing at the configured one. */
export function shouldLog(configured, level) {
    return LOG_LEVELS.indexOf(level) <= LOG_LEVELS.indexOf(configured);
}

/**
 * Receive rate between two samples of the byte counter, in KiB/s.
 *
 * The counter is reset by a reboot and wraps around on 32-bit kernels, so a
 * value lower than the previous one means a restart of the count, not negative
 * traffic: such a sample reports zero instead of a wild number.
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

/**
 * The service itself: the MQTT client, the state that has to survive between two
 * measurements, and the handlers. Laid out like the classes of the Python
 * template, so both templates read the same way.
 */
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

        // Handlers are passed to the client by reference, and a method loses its
        // this when it is. Bind once here instead of at every subscription.
        this.onConnect = this.onConnect.bind(this);
        this.onMessage = this.onMessage.bind(this);
        this.onClose = this.onClose.bind(this);
        this.onError = this.onError.bind(this);
        this.onSignal = this.onSignal.bind(this);
        this.onTick = this.onTick.bind(this);
        this.onDeviceWiped = this.onDeviceWiped.bind(this);
    }

    /** Prints a message when the configured level is talkative enough. */
    log(level, message) {
        if (shouldLog(this.config.log_level, level)) {
            console.log(message);
        }
    }

    /**
     * Publishes the device, every control with its metadata, and a clean slate of
     * errors: an error of a previous run is retained on the broker and would stay
     * on the control for good.
     */
    publishDevice() {
        this.client.publish(`${DEVICE_TOPIC}/meta`, JSON.stringify({
            driver: DEVICE.id,
            title: DEVICE.title,
        }), { retain: true });
        this.client.publish(`${DEVICE_TOPIC}/meta/error`, '', { retain: true });

        for (const [control, meta] of Object.entries(DEVICE.controls)) {
            this.client.publish(`${controlTopic(control)}/meta`, JSON.stringify(meta), { retain: true });
            this.client.publish(`${controlTopic(control)}/meta/error`, '', { retain: true });
        }
        this.rxError = false;
    }

    /**
     * Wipes the retained topics of the device, so it leaves the web interface on
     * a clean stop instead of hanging there with stale values.
     */
    wipeDevice(done) {
        for (const control of Object.keys(DEVICE.controls)) {
            this.client.publish(controlTopic(control), '', { retain: true });
            this.client.publish(`${controlTopic(control)}/meta`, '', { retain: true });
            // An error set earlier would otherwise stay on the broker as an orphan.
            this.client.publish(`${controlTopic(control)}/meta/error`, '', { retain: true });
        }
        this.client.publish(`${DEVICE_TOPIC}/meta`, '', { retain: true }, done);
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

        this.client.publish(controlTopic(CONTROL.uptime),
            String(Math.round((now - this.startedAt) / 1000)), { retain: true });

        if (rx === null) {
            // A wrong interface name is a config mistake, but the service keeps the
            // device alive and marks the control instead of dying.
            this.log('error', `no interface ${this.config.interface} in ${PROC_NET_DEV}`);
            this.setRxError(true);
            return;
        }
        this.setRxError(false);

        if (this.prevBytes !== null) {
            const speed = speedKib(this.prevBytes, rx, now - this.prevAt);

            this.peak = Math.max(this.peak, speed);
            this.client.publish(controlTopic(CONTROL.rx), String(speed), { retain: true });
            this.client.publish(controlTopic(CONTROL.peak), String(this.peak), { retain: true });

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
        this.client.publish(`${controlTopic(CONTROL.rx)}/meta/error`, failed ? 'r' : '', { retain: true });
    }

    onConnect() {
        this.connected = true;
        if (this.wasDisconnected) {
            // The broker drops the session of a client it lost, so the device has
            // to be published again after every reconnect, not just at start.
            this.log('info', 'reconnected, republishing the device');
        } else {
            this.log('info', `connected to the broker, watching ${this.config.interface}`
                + ` every ${this.config.update_interval}s`);
        }

        this.publishDevice();
        this.update();

        clearInterval(this.timer);
        this.timer = setInterval(this.onTick, this.config.update_interval * 1000);

        this.client.subscribe(RESET_TOPIC);
    }

    onTick() {
        this.update();
    }

    /**
     * Only the button is listened to, everything else is ignored: a service must
     * not react to topics it did not ask for.
     */
    onMessage(topic) {
        if (topic !== RESET_TOPIC) {
            return false;
        }
        this.peak = 0;
        this.client.publish(controlTopic(CONTROL.peak), '0', { retain: true });
        this.log('debug', 'peak reset from the web interface');
        return true;
    }

    onClose() {
        // Also fires after a failed connection attempt, when there was nothing to
        // lose yet; only a connection that existed is worth reporting.
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
        // With no broker there is nothing left to flush, so close hard instead of
        // waiting for a DISCONNECT that cannot be sent anyway.
        this.client.end(!this.connected, {}, exitSuccess);
    }

    onSignal(signal) {
        if (this.stopping) {
            return;
        }
        this.stopping = true;
        clearInterval(this.timer);
        this.log('info', `got ${signal}, stopping`);

        // Belt and braces: should a publish or the disconnect never report back,
        // the service still leaves on its own. unref() keeps this timer from
        // holding the process up when the stop goes the normal way.
        this.stopTimer = setTimeout(exitSuccess, STOP_TIMEOUT_MS);
        this.stopTimer.unref();
        this.wipeDevice(this.onDeviceWiped);
    }

    /**
     * Connects to the broker and keeps the device up to date until a signal
     * arrives. The Last Will is written before connecting, so the broker reports
     * a crash of the service on its own.
     */
    run() {
        this.client = mqtt.connect(MQTT_URL, {
            // An error topic carries the flags r, w and p, not prose: see the Wiren
            // Board conventions, section Errors. 'r' is the closest for a service that
            // died, the values it published are the last known good ones.
            will: {
                topic: `${DEVICE_TOPIC}/meta/error`, payload: 'r', retain: true, qos: 0,
            },
            // Do not keep QoS 0 messages while the broker is away. A reconnect
            // republishes the whole device, so measurements taken meanwhile are
            // worthless, and the queue they go into has no limit. It also makes
            // publish report back at once instead of never, which is what lets the
            // service stop while the broker is down instead of hanging until
            // systemd kills it.
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
    const args = parseArgs(argv);

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
        // Everything after the connect is retried by the client itself; this
        // catches what breaks before the event loop even starts.
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
