const amqp = require('amqplib');
const WebSocket = require('ws');

// Configuración de RabbitMQ
const amqpUrl = 'amqp://44.208.120.29';
const queue = 'mqtt';

let wss;

// Función para configurar el servidor WebSocket con reconexión automática
const setupWebSocketServer = () => {
  wss = new WebSocket.Server({ port: 8080 });

  wss.on('connection', (ws) => {
    console.log('New client connected');
    
    ws.on('close', () => {
      console.log('Client disconnected');
    });

    ws.on('error', (err) => {
      console.error('WebSocket error:', err);
    });
  });

  wss.on('close', () => {
    console.error('WebSocket server closed. Reconnecting...');
    setTimeout(setupWebSocketServer, 1000); // Intentar reconectar después de 1 segundo
  });

  wss.on('error', (err) => {
    console.error('WebSocket server error:', err);
    wss.close();
    setTimeout(setupWebSocketServer, 1000); // Intentar reconectar después de 1 segundo
  });

  console.log('WebSocket server started on port 8080');
};

// Función para enviar datos a todos los clientes WebSocket conectados
const broadcastData = (data) => {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
};

// Función para iniciar el consumidor de RabbitMQ con reconexión automática
const startConsumer = async () => {
  try {
    const connection = await amqp.connect(amqpUrl);
    const channel = await connection.createChannel();

    await channel.assertQueue(queue, {
      durable: true
    });

    console.log('Waiting for messages in %s. To exit press CTRL+C', queue);

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        try {
          console.log('Received:', msg.content.toString());
          const data = JSON.parse(msg.content.toString());

          // Validar datos recibidos
          if (data.temperature != null && data.soilMoisture != null && data.waterLevel != null) {
            console.log('Data is valid:', data);

            // Enviar los datos a los clientes WebSocket
            broadcastData(data);
          } else {
            console.error('Invalid data format:', data);
          }

          // Confirmar recepción del mensaje
          channel.ack(msg);
        } catch (err) {
          console.error('Error processing message:', err);
          console.error('Message content:', msg.content.toString());
          // No confirmar el mensaje para que pueda ser reintentado
        }
      }
    });

    // Manejar el cierre de la conexión
    connection.on('close', (err) => {
      console.error('Connection closed, reconnecting...', err);
      setTimeout(startConsumer, 1000); // Intentar reconectar después de 1 segundo
    });

    // Manejar errores de la conexión
    connection.on('error', (err) => {
      console.error('Connection error, reconnecting...', err);
      setTimeout(startConsumer, 1000); // Intentar reconectar después de 1 segundo
    });
  } catch (error) {
    console.error('Error in RabbitMQ consumer:', error);
    setTimeout(startConsumer, 1000); // Intentar reconectar después de 1 segundo
  }
};

// Iniciar el servidor WebSocket
setupWebSocketServer();

// Iniciar el consumidor de RabbitMQ
startConsumer();
