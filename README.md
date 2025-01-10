# rabbit-mq-consumer
Consumer of the producer repo: https://github.com/venus1344/rabbit-mq-producer

## How to run
1. Clone the repo
2. Create a new file called `application.properties` in the `src/main/resources` folder and add the following properties:
    - `rabbitmq.uri=amqp://<username>:<password>@<host>:<port>`
    - `redis.host=<host>`
    - `redis.port=<port>`
    - `postgres.url=jdbc:postgresql://<host>:<port>/<database>`
    - `postgres.user=<username>`
    - `postgres.password=<password>`
    - `encryption.key=<key>`
    - `encryption.secret=<secret>`
3. Run `mvn clean package`
4. Run `java -jar target/rabbit-mq-consumer-1.0-SNAPSHOT.jar`

#### PS: The encryption key & secret should be the same as the ones used in the producer repo.

## Logic
- The consumer will consume the messages from the RabbitMQ queue and decrypt them using the encryption key & secret.
- The decrypted message will be processed by the `MetricsWorker` class.
- The `MetricsWorker` class will store the metrics in Redis and PostgreSQL.

The redis setup is not really up to standard, it was a quick attempt at trying things out.
