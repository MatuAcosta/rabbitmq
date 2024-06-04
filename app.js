const express = require('express');
const app = express();
const producer = require('./producer');
const {
    consume,
    createChannel,
    createConnection
} = require('./consumer');


app.use(express.json())

let producerData = {
    status: 'disconnected',
    channel: null
};

app.post('/publish', async (req, res) => {
    const { test } = req.body;
    try {
        if(producerData.status === 'disconnected') {
            res.send('Producer is disconnected');
            return;
        }
        await producer.publish(producerData.channel, test);
        res.send('Published');
        
    } catch (error) {
        res.send('error');
    }
});

(async () => {
    const {connectionProducer, channelProducer} = await producer.createChannelProducer();
    connectionProducer.on('connect', () => {
        console.log('Connected Producer!');
        producerData.status = 'connected';
        producerData.channel = channelProducer;
    });
    connectionProducer.on('disconnect', err => {
        console.log('Disconnected.', err);
        producerData.status = 'disconnected';
        producerData.channel = null;
    
    });
})();

(async () => {
    const connectionConsumer = await createConnection();
    const { channelConsumer, queueSet } = await createChannel(connectionConsumer);
    connectionConsumer.on('connect', async () => {
        console.log('Connected!');
        consume(channelConsumer, queueSet);
    });
    connectionConsumer.on('disconnect', err => console.log('Disconnected.', err));
})();

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});