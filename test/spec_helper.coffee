chai = require 'chai'
should = chai.should()
global.assert = chai.assert

global._ = require 'lodash'

req = require '../mysql_esm'
global.MysqlESM = req.esm
knex = req.knex

g = require('ger')
global.GER = g.GER
NamespaceDoestNotExist = GER.NamespaceDoestNotExist

global.bb = require 'bluebird'

global.moment = require 'moment'

global._knex = knex({
  client: 'mysql',
  pool: {min: 5, max: 20},
  connection: {
    host: '127.0.0.1',
    user: 'root',
    password: '',
    timezone: 'utc',
    charset: 'utf8'
  }
})

global.default_namespace = 'default'

global.last_week = moment().subtract(7, 'days')
global.three_days_ago = moment().subtract(2, 'days')
global.two_days_ago = moment().subtract(2, 'days')
global.yesterday = moment().subtract(1, 'days')
global.soon = moment().add(50, 'mins')
global.today = moment()
global.now = today
global.tomorrow = moment().add(1, 'days')
global.next_week = moment().add(7, 'days')


global.new_esm = ()->
  new MysqlESM({knex: _knex}, NamespaceDoestNotExist)

global.init_esm = (ESM, namespace = global.default_namespace) ->
  #in
  esm = new_esm(ESM)
  #drop the current tables, reinit the tables, return the esm
  bb.try(-> esm.destroy(namespace))
  .then( -> esm.initialize(namespace))
  .then( -> esm)

global.init_ger = (ESM, namespace = global.default_namespace) ->
  init_esm(ESM, namespace)
  .then( (esm) -> new GER(esm))
