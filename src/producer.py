import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from mastodon import Mastodon, MastodonNetworkError
from src.common import csv_setting, keyword_pattern, normalize
from src.listener import PublicListener

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--demo', action='store_true')
    args = parser.parse_args()
    token = os.getenv('MASTODON_ACCESS_TOKEN')
    if not args.demo and not token:
        parser.error('Renseigner MASTODON_ACCESS_TOKEN dans .env ou utiliser --demo.')
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        acks='all', retries=10, max_in_flight_requests_per_connection=1,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
    topic = os.getenv('KAFKA_TOPIC', 'mastodon_stream')
    words = csv_setting('KEYWORDS', 'AI,DataScience')
    pattern = re.compile(keyword_pattern(words)) if words else None

    def publish(event):
        if event and (not pattern or pattern.search(event['text'] + ' ' + ' '.join(event['hashtags']))):
            # Attendre l'accusé du broker
            producer.send(topic, key=(event['instance'] + '/' + event['id']).encode(), value=event).get(timeout=30)
            log.info('Message envoyé : %s', event['id'])

    try:
        if args.demo:
            # Identifiants fixes : relancer la démo vérifie aussi la déduplication.
            for i in range(12):
                publish(dict(id=f'demo-{i}', instance='demo', timestamp=datetime.now(timezone.utc).isoformat(),
                             text=f'Exemple {i} : music, AI et DataScience avec Spark', user_id=f'user-{i % 3}',
                             username=f'demo{i % 3}', language='fr', hashtags=['music', 'ai', 'datascience'],
                             location=None, favourites_count=0, reblogs_count=0))
                time.sleep(1)
        else:
            instance = os.getenv('MASTODON_API_BASE_URL', 'https://mastodon.social')
            api = Mastodon(access_token=token, api_base_url=instance, ratelimit_method='wait')

            tags = csv_setting('MASTODON_HASHTAGS', '')
            if tags:
                handles = []
                listeners = []
                try:
                    for tag in tags:
                        listener = PublicListener(lambda status: publish(normalize(status, instance)))
                        listeners.append(listener)
                        handles.append(api.stream_hashtag(
                            tag, listener, run_async=True, timeout=60,
                            reconnect_async=True, reconnect_async_wait_sec=15))
                        log.info('Abonnement au flux #%s', tag)
                    while True:
                        time.sleep(30)
                        if not all(handle.is_alive() for handle in handles):
                            raise RuntimeError('Un flux hashtag est arrêté ; consulter les erreurs précédentes.')
                        log.info('Flux hashtags : %s/%s connectés, %s publications reçues avant filtrage',
                                 sum(handle.is_receiving() for handle in handles), len(handles),
                                 sum(listener.received for listener in listeners))
                finally:
                    for handle in handles:
                        handle.close()
                return

            listener = PublicListener(lambda status: publish(normalize(status, instance)))
            while True:
                try:
                    log.info('Connexion au flux public de %s', instance)
                    api.stream_public(listener, timeout=60)
                except MastodonNetworkError:
                    log.warning('Flux interrompu, reconnexion dans 15 secondes.')
                time.sleep(15)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
