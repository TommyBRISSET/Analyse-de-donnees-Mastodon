"""Contrat JSON commun au producteur et au consommateur."""
import os
import re
from html.parser import HTMLParser


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ('p', 'br', 'div'):
            self.parts.append(' ')

    def handle_endtag(self, tag):
        if tag in ('p', 'div'):
            self.parts.append(' ')


def clean_html(content):
    parser = TextParser()
    parser.feed(content or '')
    return ' '.join(''.join(parser.parts).split())


def csv_setting(name, default):
    return [v.strip().lower() for v in os.getenv(name, default).split(',') if v.strip()]


def keyword_pattern(words):
    # Les limites de mots empêchent par exemple « AI » de sélectionner « travail ».
    return r'(?iu)\b(?:' + '|'.join(re.escape(w.lstrip('#')) for w in words) + r')\b'


def normalize(status, instance):
    if status.get('reblog') or status.get('visibility') != 'public':
        return None
    created = status['created_at']
    return dict(id=str(status['id']), instance=instance.rstrip('/'),
                timestamp=created.isoformat() if hasattr(created, 'isoformat') else created,
                text=clean_html(status.get('content')), user_id=str(status['account']['id']),
                username=status['account']['acct'], language=status.get('language'),
                hashtags=sorted({t['name'].lower() for t in status.get('tags', [])}),
                location=None, favourites_count=int(status.get('favourites_count', 0)),
                reblogs_count=int(status.get('reblogs_count', 0)))
