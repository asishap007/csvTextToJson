/**
 * Created by asishap on 15/12/16.
 */

var fs = require("fs"),
    _ = require("extended")().register(require("is-extended")).register(require("object-extended")),
    EventEmitter = require("events").EventEmitter,
    util = require("util"),
    out = process.stdout,
    Stream = require("stream").Stream;


var VALIDATE = '^\\s*(?:\'[^\'\\\\]*(?:\\\\[\\S\\s][^\'\\\\]*)*\'|"[^"\\\\]*(?:\\\\[\\S\\s][^"\\\\]*)*"|[^,\'"\\s\\\\]*(?:\\s+[^,\'"\\s\\\\]+)*)\\s*(?:,\\s*(?:\'[^\'\\\\]*(?:\\\\[\\S\\s][^\'\\\\]*)*\'|"[^"\\\\]*(?:\\\\[\\S\\s][^"\\\\]*)*"|[^,\'"\\s\\\\]*(?:\\s+[^,\'"\\s\\\\]+)*)\\s*)*$';
var EMPTY = /^\s*(?:''|"")?\s*(?:,\s*(?:''|"")?\s*)*$/;
var VALUE = '(?!\\s*$)\\s*(?:\'([^\'\\\\]*(?:\\\\[\\S\\s][^\'\\\\]*)*)\'|"([^"\\\\]*(?:\\\\[\\S\\s][^"\\\\]*)*)"|([^,\'"\\s\\\\]*(?:\\s+[^,\'"\\s\\\\]+)*))\\s*(?:,|$)';
var LINE_SPLIT = /[\r\n|\r|\n]/;

function Parser(options) {
    var validate = VALIDATE
        ,value = VALUE
        ,nonTabWhiteSpace = ' \\f\\v​\\u00a0\\u1680​\\u180e\\u2000​\\u2001\\u2002​\\u2003\\u2004​\\u2005\\u2006​\\u2007\\u2008​\\u2009\\u200a​\\u2028\\u2029​​\\u202f\\u205f​\\u3000'
        ,nonTabWhiteSpaceOr = '(?: |\\f|\\v|​\\u00a0|\\u1680|​\\u180e|\\u2000|​\\u2001|\\u2002|​\\u2003|\\u2004|​\\u2005|\\u2006​|\\u2007|\\u2008​|\\u2009|\\u200a​|\\u2028|\\u2029​​|\\u202f|\\u205f​|\\u3000)'
        ;
    EventEmitter.call(this);
    this._parsedHeaders = false;
    this._rowCount = 0;
    options = options || {};
    this._headers = options.isHeaders;
    this._ignoreEmpty = options.isIgnoreEmpty;
    this._delimiter = typeof(options.delimiter) == 'string'
        ? options.delimiter.replace(/[-[\]{}()*+?.\\^$|#\s]/g, "\\$&") // escape regex control chars
        : ',';
    if (this._delimiter == '\\\t') { // tab is the only viable whitespace delimiter
        this._delimiter = '\\t';
        validate = validate.replace(/(\[[^\s\]]*)\\s([^\]]*\])/g,'$1'+nonTabWhiteSpace+'$2').replace(/\\s/g,nonTabWhiteSpaceOr);
        value = value.replace(/(\[[^\s\]]*)\\s([^\]]*\])/g,'$1'+nonTabWhiteSpace+'$2').replace(/\\s/g,nonTabWhiteSpaceOr);
    }
    this._VALUE = new RegExp(value.replace(/,/g,this._delimiter),'g');
    this._VALIDATE = new RegExp(validate.replace(/,/g,this._delimiter));
}
util.inherits(Parser, EventEmitter);

_(Parser).extend({
    __parseLine: function __parseLineData(data, index, ignore) {

        var ignoreEmpty = this._ignoreEmpty;
        if (!this._VALIDATE.test(data)) {
            this.emit("error", new Error("Invalid row " + data));
            return null;
        } else if (_.isBoolean(ignoreEmpty) && ignoreEmpty && EMPTY.test(data)) {
            return null;
        }
        var a = [];
        data.replace(this._VALUE, function lineReplace(m0, m1, m2, m3) {
            var item;
            if (m1 !== undefined) {
                item = m1.replace(/\\'/g, "'");
            } else if (m2 !== undefined) {
                item = m2.replace(/\\"/g, '"');
            } else if (m3 !== undefined) {
                item = m3;
            }
            a.push(item);
            return ''; // Return empty string.
        }.bind(this));
        // Handle special case of empty last value.
        if ((new RegExp(this._delimiter+'\\s*$')).test(data)) {
            a.push('');
        }
        if (!ignore) {
            a = this._transform(a, index);
            if (this._validate(a, index)) {
                return a;
            } else {
                this.emit("data-invalid", a, index);
            }
        } else {
            return a;
        }
    },

    _parse: function _parseLine(data) {
        var row, parseLine = this.__parseLine.bind(this), emitRow = this.emit.bind(this, "data"), count = 0;
        if (!this._parsedHeaders) {
            var headers = this._headers;
            if (_.isBoolean(headers) && headers) {
                headers = parseLine(data.shift(), 0, true);
            }
            if (_.isArray(headers)) {
                var headersLength = headers.length,
                    orig = this._transform.bind(this);
                this._transform = function (data, index) {
                    var ret = {};
                    for (var i = 0; i < headersLength; i++) {
                        ret[headers[i]] = data[i];
                    }
                    return orig(ret, index);
                };
            }
            this._parsedHeaders = true;
        }
        for (var i = 0, l = data.length; i < l; i++) {
            row = data[i];
            if (row) {
                var dataRow = parseLine(row, count);
                if (dataRow) {
                    count = this._rowCount++;
                    emitRow(dataRow, count);
                }
            }
        }
    },
    sourceStream: function _from(from) {
        this.__from = from;
        return this;
    },
    dataToJson: function _parse(from) {
        from = from || this.__from;
        if (_.isString(from)) {
            from = fs.createReadStream(from);
            from.on("end", function () {
                from.destroy();
            });
        }
        if (_.isObject(from) && from instanceof Stream) {
            var lines = "", parse = this._parse.bind(this), end = this.emit.bind(this, "end");
            from.on("data", function streamOnData(data) {

                /**
                 * Fix for node version less than 4.4.
                 * @type {string}
                 */
                var tLine = ""+data;
                var firstChar = tLine.substring(0,1);
                if(firstChar === '"' && lines){
                    data = '\t'+ data;
                }
                //---------------------

                var lineData = (lines + data).trim().split(LINE_SPLIT);
                if (lineData.length > 1) {
                    lines = lineData.pop();
                    parse(lineData);
                } else {
                    lines += data;
                }
            });
            from.on("end", function streamOnEnd() {
                parse(lines.split(LINE_SPLIT));
                end(this._rowCount);
            }.bind(this));
        } else {
            throw new TypeError("csvtxt-convert.Parser#parse from must be a path or ReadableStream");
        }
        return this;
    },

    _validate: function (data, index) {
        return true;
    },
    _transform: function (data, index) {
        return data;
    },
    validateData: function (cb) {
        if (!_.isFunction(cb)) {
            throw new TypeError("csvtxt-convert.Parser#validate requires a function");
        }
        this._validate = cb;
        return this;
    },
    transformData: function (cb) {
        if (!_.isFunction(cb)) {
            throw new TypeError("csvtxt-convert.Parser#transform requires a function");
        }
        this._transform = cb;
        return this;
    }
});

/**
 * Entry point to `csvtxt-convert`. `csvtxt-convert` does not store rows its proccesses each row and emits it. If you wish to save
 * every row into an array you must store them yourself by using the `data` event. Once all rows are done processing
 * the `end` event is emitted.
 *
 * Invoke to parse a csv file.
 *
 * @name csvtxt-convert
 * @param location
 * @param options
 * @return {*}
 */
module.exports = function parse(location, options) {
    return new Parser(options).sourceStream(location);
};