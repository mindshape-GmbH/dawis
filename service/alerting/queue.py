from database import MongoDB
from datetime import datetime
from typing import Sequence


class Alert:
    date: datetime
    group: str
    message: str
    data: dict

    def __init__(self, date: datetime, group: str, message: str, data: dict = None):
        if data is None:
            data = {}

        self.date = date
        self.group = group
        self.message = message
        self.data = data

    def to_dict(self) -> dict:
        return {
            'date': self.date,
            'group': self.group,
            'message': self.message,
            'data': self.data,
        }


class AlertQueue:
    COLLECTION_ALERT_QUEUE = 'alert_queue'

    _mongodb: MongoDB

    def __init__(self, mongodb_connection: MongoDB):
        self._mongodb = mongodb_connection

    def add_alerts(self, alerts: Sequence[Alert]):
        if 0 < len(alerts):
            self._mongodb.insert_documents(self.COLLECTION_ALERT_QUEUE, [alert.to_dict() for alert in alerts])

    def add_alert(self, alert: Alert):
        self._mongodb.insert_document(self.COLLECTION_ALERT_QUEUE, alert.to_dict())

    def fetch_alerts(self, groups: Sequence[str], delete: bool = True, limit: int = 0) -> Sequence[Alert]:
        alerts = []

        if not self._mongodb.has_collection(self.COLLECTION_ALERT_QUEUE):
            return alerts

        for alert in self._mongodb.find(
            self.COLLECTION_ALERT_QUEUE,
            {'$or': [{'group': group for group in groups}]} if 1 < len(groups) else {'group': groups[0]},
            True,
            limit
        ):
            alerts.append(Alert(alert['date'], alert['group'], alert['message'], alert['data']))

            if delete:
                self._mongodb.delete_one(self.COLLECTION_ALERT_QUEUE, alert['_id'])

        return alerts
