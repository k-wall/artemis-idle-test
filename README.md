# artemis-idle-test

Test utility to explore the AMQP idle behaviour of the Artemis broker for inbound AMQP connections and outbound connections with the EnMasse broker plugin.

Starts a server that will accept incoming connections and send an Open only.
```
App --server 15672
```

Starts a server that will accept incoming connections and send an Open only,  respond Close to a solicited Close and shutdown the socket.
```
App --server 15672 --answer-close --shutdown-socket
```
