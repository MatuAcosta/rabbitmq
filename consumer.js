const amqp = require('amqp-connection-manager')
const exchange = "product_inventory";


const createConnection = async () => {
    try {
        const connection = amqp.connect("amqp://localhost?heartbeat=30s");
        return connection;
    } catch (error) {
        console.log('Error cronnection', error);
    }
}

const createChannel = async (connection) => {
try {
    let queueSet;
    const channel = connection.createChannel({
        json: true,
        setup: function (ch) {
            ch.assertExchange(exchange, 'direct', { durable: true })
            const { queue } = channel.assertQueue('test', { durable: true });
            queueSet = queue;
            ch.bindQueue(queue, exchange, 'test');
            ch.prefetch(1);
            return ch;
        }
    });
    return {
        channelConsumer: channel,
        queueSet
    };
} catch (error) {
    console.log('Error cronnection', error);
}

}


const consume = async (channel, queue) => {
    try {
        console.log('Waiting for messages');
        channel.consume(queue, async (message) => {
                const {fields, content } = message;
                if(fields.redelivered){
                    console.log('Message redelivered');
                    channel.ack(message);  
                } else {
                    const messageParsed = JSON.parse(content.toString());
                    setTimeout(() => {
                        message.fail ? channel.nack(message) : channel.ack(message);
                    }, 1000);
                    console.log('Message received', messageParsed);
                }                
        });
    } catch (err) {
      console.log('ERROR EN CONSUMER', err);
      return err;
    }
}

module.exports = {
    createConnection,
    createChannel,
    consume,

} 