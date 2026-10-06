from mastodon import StreamListener


class PublicListener(StreamListener):
    def __init__(self, on_status):
        super().__init__()
        self.on_status = on_status
        self.received = 0

    def _dispatch(self, event):
        if not event:
            return
        return super()._dispatch(event)

    def on_update(self, status):
        self.received += 1
        self.on_status(status)
