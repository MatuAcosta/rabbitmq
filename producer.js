const amqp = require('amqp-connection-manager')
const exchange = "product_inventory";
const text = {
  item_id: "macbook",
  text: "This is a sample message to send receiver to check the ordered Item Availablility",
};

exports.createChannelProducer = async () => {
  try {
      const connection = amqp.connect("amqp://localhost?heartbeat=30s");
      const channel = connection.createChannel({
          json: true,
          setup: channel => {
              channel.assertExchange(exchange, 'direct', { durable: true })
              const { queue } = channel.assertQueue('test', { durable: true });
              channel.bindQueue(queue, exchange, 'test');
              channel.prefetch(1);
          }
      });
      return {
          connectionProducer: connection,
          channelProducer: channel
      };
  } catch (error) {
      console.log('Error cronnection', error);
  }
  
  }


exports.publish = async (channel, data) => {
  try {
    data.forEach(async (message, index) => {
      await channel.publish(exchange, 'test', message , { persistent: true });
      console.log(' [x] Sent "%s"', message);
    });

  } catch (error) {
    console.log('ERROR EN PRODUCER', err);

  }
}
