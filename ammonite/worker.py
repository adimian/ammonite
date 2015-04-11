import pika
import argparse
import ConfigParser
import os


def serve(config):
    parameters = pika.ConnectionParameters(host=config.get('AMQP', 'HOSTNAME'))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = config.get('QUEUES', 'JOBS')

    channel.queue_declare(queue=queue_name, durable=True)
    print '---'
    print '--- Ammonite worker ready. Waiting for messages. To exit press CTRL+C'
    print '---'

    def callback(ch, method, properties, body):
        print 'received %r' % (body,)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=int(config.get('WORKER', 'SLOTS')))
    channel.basic_consume(callback,
                          queue=queue_name)

    channel.start_consuming()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--config-file', type=str,
                        dest='config', help='configuration file')
    args = parser.parse_args()

    config_file = args.config
    if config_file is None:
        parser.error('no configuration file provided')
    if not os.path.exists(config_file):
        parser.error('configuration file %s does not exist' % config_file)

    config_parser = ConfigParser.ConfigParser()
    config_parser.read([str(config_file), ])

    serve(config_parser)
