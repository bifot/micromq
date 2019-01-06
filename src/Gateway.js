const http = require('http');
const nanoid = require('nanoid');
const qs = require('querystring');
const parse = require('co-body');
const BaseService = require('./BaseService');
const Route = require('./Route');

class Gateway {
  constructor(options) {
    this.options = options;

    this._consumersReady = false;
    this._requests = new Map();
    this._middlewares = [];
    this._microservices = options.microservices.reduce((object, name) => ({
      ...object,
      [name]: new BaseService({
        rabbit: options.rabbit,
        name,
      }),
    }), {});
  }

  async _startConsumers() {
    if (this._consumersReady) {
      return;
    }

    await Promise.all(
      Object.values(this._microservices).map(async (microservice) => {
        const responsesChannel = await microservice.createResponsesChannel();

        responsesChannel.consume(microservice.responsesQueueName, (message) => {
          if (!message) {
            return;
          }

          const { response, statusCode, headers, requestId } = JSON.parse(message.content.toString());
          const res = this._requests.get(requestId);

          if (!res || !response) {
            return;
          }

          res.writeHead(statusCode, headers);
          res.end(response);

          responsesChannel.ack(message);

          this._requests.delete(requestId);
        });
      }),
    );

    this._consumersReady = true;
  }

  use(middleware) {
    this._middlewares.push(middleware);
  }

  async listen(port) {
    await this._startConsumers();

    const route = new Route(undefined, undefined, this._middlewares);

    return http
      .createServer(async (req, res) => {
        const [path, queryString] = req.url.split('?');
        const query = qs.decode(queryString);
        const body = await parse.json(req);

        req.body = body;
        req.query = query;
        req.session = {};

        res.delegate = async (name) => {
          const microservice = this._microservices[name];

          if (!microservice) {
            throw new Error(`Microservice ${name} not found`);
          }

          const requestsChannel = await microservice.createResponsesChannel();

          const message = {
            path,
            method: req.method.toLowerCase(),
            payload: {
              query,
              body,
              headers: req.headers,
              session: req.session,
            },
            requestId: nanoid(),
          };

          this._requests.set(message.requestId, res);

          requestsChannel.sendToQueue(microservice.requestsQueueName, Buffer.from(JSON.stringify(message)));
        };

        route._next(req, res);
      })
      .listen(port);
  }
}

module.exports = Gateway;