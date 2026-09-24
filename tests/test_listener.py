import unittest
from unittest.mock import Mock
from mastodon import MastodonMalformedEventError
from src.listener import PublicListener


class ListenerTest(unittest.TestCase):
    def test_empty_event_does_not_interrupt_stream(self):
        callback = Mock()
        listener = PublicListener(callback)
        listener._dispatch({})
        callback.assert_not_called()
        self.assertEqual(listener.received, 0)
        listener._dispatch({'event': 'update', 'data': '{"id": "123", "content": "AI"}'})
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(listener.received, 1)
        self.assertEqual(callback.call_args[0][0]['content'], 'AI')

    def test_invalid_nonempty_event_is_not_silenced(self):
        with self.assertRaises(MastodonMalformedEventError):
            PublicListener(Mock())._dispatch({'event': 'update'})


if __name__ == '__main__':
    unittest.main()
