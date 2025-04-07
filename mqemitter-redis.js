'use strict'

const Redis = require('ioredis')
const MQEmitter = require('mqemitter')
const hyperid = require('hyperid')()
const { LRUCache } = require('lru-cache')
const msgpack = require('msgpack-lite')
const { EventEmitter } = require('events')

class MQEmitterRedis extends MQEmitter {
  constructor (opts) {
    opts = { ...opts }

    opts.enableAutoPipelining = true
    super(opts)
    this._opts = opts

    this.subConn = opts.subConn || new Redis(opts.connectionString || opts)
    this.pubConn = opts.pubConn || new Redis(opts.connectionString || opts)

    this._topics = {}

    this._cache = new LRUCache({
      max: opts.maxLRUCache || 10000, // default: 10k
      ttl: opts.ttlLRUCache || 60 * 1000 // default: one minute
    })

    this.state = new EventEmitter()

    const that = this

    const onError = (err) => {
      if (err && !this.closing) {
        this.state.emit('error', err)
      }
    }

    this._onError = onError

    const handler = (sub, topic, payload) => {
      const packet = msgpack.decode(payload)
      if (!this._cache.get(packet.id)) {
        super.emit(packet.msg)
      }
      this._cache.set(packet.id, true)
    }

    this.subConn.on('messageBuffer', (topic, message) => {
      handler(topic, topic, message)
    })

    this.subConn.on('pmessageBuffer', (sub, topic, message) => {
      handler(sub, topic, message)
    })

    this.subConn.on('connect', () => {
      that.state.emit('subConnect')
    })

    this.subConn.on('error', err => {
      this._onError(err)
    })

    this.pubConn.on('connect', () => {
      this.state.emit('pubConnect')
    })

    this.pubConn.on('error', err => {
      this._onError(err)
    })

    this._opts.regexWildcardOne = new RegExp(this._opts.wildcardOne.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g')
    this._opts.regexWildcardSome = new RegExp((this._opts.matchEmptyLevels ? this._opts.separator.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&') + '?' : '') + this._opts.wildcardSome.replace(/([/,!\\^${}[\]().*+?|<>\-&])/g, '\\$&'), 'g')
  }

  close (cb) {
    cb = cb || noop

    if (this.closed || this.closing) {
      return cb()
    }

    this.closing = true

    let count = 2

    const onEnd = () => {
      if (--count === 0) {
        super.close(cb)
      }
    }

    this.subConn.on('end', onEnd)
    this.subConn.quit()

    this.pubConn.on('end', onEnd)
    this.pubConn.quit()

    return this
  }

  _subTopic (topic) {
    return topic
      .replace(this._opts.regexWildcardOne, '*')
      .replace(this._opts.regexWildcardSome, '*')
  }

  on (topic, cb, done) {
    const subTopic = this._subTopic(topic)
    const onFinish = () => {
      if (done) {
        setImmediate(done)
      }
    }

    super.on(topic, cb)

    if (this._topics[subTopic]) {
      this._topics[subTopic]++
      onFinish.call(this)
      return this
    }

    this._topics[subTopic] = 1

    if (this._containsWildcard(topic)) {
      this.subConn.psubscribe(subTopic, onFinish.bind(this))
    } else {
      this.subConn.subscribe(subTopic, onFinish.bind(this))
    }

    return this
  }

  emit (msg, done) {
    console.log('emit', msg)
    done = done || this._onError

    if (this.closed) {
      const err = new Error('mqemitter-redis is closed')
      return done(err)
    }

    const packet = {
      id: hyperid(),
      msg
    }

    this.pubConn.publish(msg.topic, msgpack.encode(packet)).then(() => done()).catch(done)
  }

  removeListener (topic, cb, done) {
    const subTopic = this._subTopic(topic)
    const onFinish = () => {
      if (done) {
        setImmediate(done)
      }
    }

    super.removeListener(topic, cb)

    if (--this._topics[subTopic] > 0) {
      onFinish()
      return this
    }

    delete this._topics[subTopic]

    if (this._containsWildcard(topic)) {
      this.subConn.punsubscribe(subTopic, onFinish)
    } else if (this._matcher.match(topic)) {
      this.subConn.unsubscribe(subTopic, onFinish)
    }

    return this
  }

  _containsWildcard (topic) {
    return (topic.indexOf(this._opts.wildcardOne) >= 0) ||
           (topic.indexOf(this._opts.wildcardSome) >= 0)
  }
}

function noop () {}

module.exports = (opts) => new MQEmitterRedis(opts)
module.exports.MQEmitterRedis = MQEmitterRedis

class MQEmitterRedisPrefix extends MQEmitterRedis {
  constructor (pubSubPrefix, options) {
    super(options)
    this._pubSubPrefix = pubSubPrefix
    this._sym_proxiedCallback = Symbol('proxiedCallback')
  }

  on (topic, cb, done) {
    const t = this._pubSubPrefix + topic
    cb[this._sym_proxiedCallback] = (packet, cbcb) => {
      const t = packet.topic.slice(this._pubSubPrefix.length)
      const p = { ...packet, topic: t }
      return cb.call(this, p, cbcb)
    }
    return super.on(t, cb[this._sym_proxiedCallback], done)
  }

  removeListener (topic, func, done) {
    const t = this._pubSubPrefix + topic
    const f = func[this._sym_proxiedCallback]
    return super.removeListener(t, f, done)
  }

  emit (packet, done) {
    const t = this._pubSubPrefix + packet.topic
    const p = { ...packet, topic: t }
    return super.emit(p, done)
  }
}

module.exports.MQEmitterRedisPrefix = MQEmitterRedisPrefix
